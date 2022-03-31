/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using QuantConnect.Configuration;
using QuantConnect.DataSource;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// CryptoCoarseFundamentalUniverseDataConverter implementation.
    /// </summary>
    public class CryptoCoarseFundamentalUniverseDataConverter
    {
        private readonly string _market;
        private readonly string _baseFolder;
        private readonly string _destinationFolder;

        private readonly DateTime _fromDate;
        private readonly DateTime _toDate;

        private Dictionary<string, Dictionary<string, string>> dataByDate = new();

        /// <summary>
        /// Creates a new instance of <see cref="CryptoCoarseFundamental"/>
        /// </summary>
        /// <param name="market">The data vendor market</param>
        /// <param name="baseFolder">The folder where the base data saved at</param>
        /// <param name="fromDate">The start date of data processing</param>
        /// <param name="toDate">The end date of data processing</param>
        public CryptoCoarseFundamentalUniverseDataConverter(string market, string baseFolder, DateTime fromDate, DateTime toDate)
        {
            _market = market;
            _baseFolder = baseFolder;
            _destinationFolder = Path.Combine(baseFolder, "fundamental", "coarse");
            _fromDate = fromDate;
            _toDate = toDate;

            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Load the local base trade data files, convert into universe file
        /// </summary>
        public bool ConvertToUniverseFile()
        {
            try
            {
                // Get base currency for reference of USD dollar volume conversion
                var baseCurrency = new Dictionary<string, string>();

                var symbolProperties = ReadOnlineDocumentLines("https://raw.githubusercontent.com/QuantConnect/Lean/master/Data/symbol-properties/symbol-properties-database.csv");
                foreach (var line in symbolProperties)
                {
                    var lineItem = line.Split(",");
                    if (lineItem.First() == _market)
                    {
                        baseCurrency[lineItem[1]] = lineItem[4];
                    }
                }

                var files = new List<string>();

                // Get all trade data files in set crypto market data file
                foreach(var file in Directory.GetFiles(Path.Combine(_baseFolder, "daily"), "*_trade.zip", SearchOption.AllDirectories))
                {
                    files.Add(file);
                }

                foreach (var fileString in files)
                {
                    string name;

                    // Fetch price data from zip files
                    using (ZipArchive zip = ZipFile.Open(fileString, ZipArchiveMode.Read))
                    {
                        foreach (ZipArchiveEntry entry in zip.Entries)
                        {
                            name = entry.Name.Split(".").First().ToUpper();

                            var streamFile = entry.Open();
                            var lines = ReadLines(streamFile);

                            foreach (var line in lines)
                            {
                                var csv = line.Split(",");
                                var dateTime = Parse.DateTimeExact(csv[0].Split(" ").First(), "yyyyMMdd");
                                if (dateTime < _fromDate || dateTime > _toDate)
                                {
                                    continue;
                                }

                                var date = $"{dateTime:yyyyMMdd}";
                                var high = decimal.Parse(csv[2], NumberStyles.Any, CultureInfo.InvariantCulture);
                                var low = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture);
                                var price = decimal.Parse(csv[4], NumberStyles.Any, CultureInfo.InvariantCulture);
                                var volume = decimal.Parse(csv[5], NumberStyles.Any, CultureInfo.InvariantCulture);
                                var dollarVolume = price * volume;

                                var content = new Dictionary<string, string>();
                                dataByDate.TryAdd(date, content);
                                if (dataByDate[date].TryGetValue(name, out var tempData))
                                {
                                    var temp = tempData.Split(",");
                                    var oldVolume = decimal.Parse(temp[1], NumberStyles.Any, CultureInfo.InvariantCulture);
                                    var oldDollarVolume = decimal.Parse(temp[2], NumberStyles.Any, CultureInfo.InvariantCulture);
                                    var oldHigh = decimal.Parse(temp[4], NumberStyles.Any, CultureInfo.InvariantCulture);
                                    var oldLow = decimal.Parse(temp[5], NumberStyles.Any, CultureInfo.InvariantCulture);

                                    var newVolume = oldVolume + volume;
                                    var newDollarVolume = oldDollarVolume + newVolume * price;
                                    var newHigh = Math.Max(oldHigh, high);
                                    var newLow = Math.Min(oldLow, low);

                                    dataByDate[date][name] = $"{price},{newVolume},{newDollarVolume},{temp[3]},{newHigh},{newLow},";
                                }
                                else
                                {
                                    dataByDate[date][name] = $"{price},{volume},{dollarVolume},{csv[1]},{high},{low},";
                                }
                            }
                        }
                    }
                }

                foreach (var fileName in dataByDate.Keys.ToList())
                {
                    var coarseByDate = dataByDate[fileName];

                    foreach (var dataTicker in coarseByDate.Keys.ToList())
                    {
                        string usdDollorVol;

                        // To USD dollar volume conversion, if <ticker>-USD/BUSD/USDT pair exist
                        if (dataTicker.Substring(dataTicker.Length - 4) != "USDT" && dataTicker.Substring(dataTicker.Length - 3) != "USD")
                        {
                            string refCurrency;
                            decimal rate = 1m;

                            if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}USD"))
                            {
                                refCurrency = $"{baseCurrency[dataTicker]}USD";
                            }
                            else if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}BUSD"))
                            {
                                refCurrency = $"{baseCurrency[dataTicker]}BUSD";
                            }
                            else if (coarseByDate.ContainsKey($"USDT{baseCurrency[dataTicker]}"))
                            {
                                rate = coarseByDate.ContainsKey("USDTUSD") ? 
                                    decimal.Parse(coarseByDate["USDTUSD"].Split(",").First(), NumberStyles.Any, CultureInfo.InvariantCulture) :
                                    1m / decimal.Parse(coarseByDate["BUSDUSDT"].Split(",").First(), NumberStyles.Any, CultureInfo.InvariantCulture);
                                refCurrency = $"USDT{baseCurrency[dataTicker]}";
                            }
                            else
                            {
                                continue;
                            }

                            var conversionRate = decimal.Parse(coarseByDate[refCurrency].Split(",").First(), NumberStyles.Any, CultureInfo.InvariantCulture);
                            conversionRate = refCurrency.Substring(0, 4) == "USDT" ? 1m / conversionRate : conversionRate;
                            var baseDollarVol = decimal.Parse(coarseByDate[dataTicker].Split(",")[2], NumberStyles.Any, CultureInfo.InvariantCulture);
                            usdDollorVol = $"{baseDollarVol * conversionRate * rate}";
                        }
                        else
                        {
                            usdDollorVol = $"{coarseByDate[dataTicker].Split(",")[2]}";
                        }

                        dataByDate[fileName][dataTicker] = $"{dataByDate[fileName][dataTicker]}{usdDollorVol}";
                    }

                    // Save to file
                    var fileContent = dataByDate[fileName].Select(x => $"{x.Key},{x.Value}").ToList();
                    var finalPath = Path.Combine(_destinationFolder, $"{fileName}.csv");
                    var finalFileExists = File.Exists(finalPath);

                    var lines = new HashSet<string>(fileContent);
                    if (finalFileExists)
                    {
                        foreach (var line in File.ReadAllLines(finalPath))
                        {
                            lines.Add(line);
                        }
                    }

                    var finalLines = lines.OrderBy(x => x.Split(',').First()).ToList();

                    var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
                    File.WriteAllLines(tempPath, finalLines);
                    var tempFilePath = new FileInfo(tempPath);
                    tempFilePath.MoveTo(finalPath, true);
                }
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"CryptoCoarseFundamentalUniverseDataConverter.ConvertToUniverseFile(): Finished Processing");
            return true;
        }

        /// <summary>
        /// Helper method to read lines in a online file.
        /// </summary>
        /// <param name="link">The link of online file to be read</param>
        public IEnumerable<string> ReadOnlineDocumentLines(string link)
        {
            WebClient client = new WebClient();
            Stream stream = client.OpenRead(link);
            return ReadLines(stream);
        }

        /// <summary>
        /// Helper method to read lines in a stream.
        /// </summary>
        /// <param name="fileStream">The stream instance to be read</param>
        public IEnumerable<string> ReadLines(Stream fileStream)
        {
            using (var reader = new StreamReader(fileStream))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    yield return line;
                }
            }
        }
    }
}