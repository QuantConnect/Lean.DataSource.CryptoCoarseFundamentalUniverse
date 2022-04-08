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
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
using QuantConnect.DataSource;
using QuantConnect.Logging;
using QuantConnect.Util;
using QuantConnect.ToolBox;

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

        private Dictionary<string, Dictionary<string, List<decimal?>>> _dataByDate = new();

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
                var baseCurrency = new Dictionary<string, string>();

                var symbolProperties = Extensions.DownloadData("https://raw.githubusercontent.com/QuantConnect/Lean/master/Data/symbol-properties/symbol-properties-database.csv")
                    .Split("\n");
                foreach (var line in symbolProperties)
                {
                    var lineItem = line.Split(",");
                    if (lineItem.First() == _market)
                    {
                        CurrencyPairUtil.DecomposeCurrencyPair(Symbol.Create(lineItem[1], SecurityType.Crypto, _market), out var __, out var quoteCurrency);
                        baseCurrency[lineItem[1]] = quoteCurrency;
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
                    var dataReader = new LeanDataReader(fileString);
                    var baseData = dataReader.Parse();

                    string name = fileString.Split("\\").Last().Split("_").First().ToUpper();

                    foreach (TradeBar data in baseData)
                    {
                        var dateTime = data.EndTime;
                        if (dateTime < _fromDate || dateTime > _toDate)
                        {
                            continue;
                        }

                        var date = $"{dateTime:yyyyMMdd}";
                        var high = data.High;
                        var low = data.Low;
                        var price = data.Close;
                        var volume = data.Volume;
                        var dollarVolume = price * volume;

                        var content = new Dictionary<string, List<decimal?>>();
                        _dataByDate.TryAdd(date, content);
                        if (_dataByDate[date].TryGetValue(name, out var temp))
                        {
                            var oldVolume = temp[1];
                            var oldDollarVolume = temp[2];
                            var oldHigh = temp[4];
                            var oldLow = temp[5];

                            var newVolume = oldVolume + volume;
                            var newDollarVolume = oldDollarVolume + newVolume * price;
                            var newHigh = high > oldHigh ? high : oldHigh;
                            var newLow = low < oldLow ? low : oldLow;

                            _dataByDate[date][name] = new List<decimal?>{price, newVolume, newDollarVolume, temp[3], newHigh, newLow};
                        }
                        else
                        {
                            _dataByDate[date][name] = new List<decimal?>{price, volume, dollarVolume, data.Open, high,low};
                        }
                    }
                }

                foreach (var fileName in _dataByDate.Keys.ToList())
                {
                    var coarseByDate = _dataByDate[fileName];

                    foreach (var dataTicker in coarseByDate.Keys.ToList())
                    {
                        var usdDollarVol = GetUSDDollarVolume(coarseByDate, baseCurrency, dataTicker);

                        _dataByDate[fileName][dataTicker].Add(usdDollarVol);
                    }

                    // Save to file
                    var fileContent = _dataByDate[fileName].Select(x => 
                        {
                            var ticker = x.Key;
                            var sid = SecurityIdentifier.GenerateCrypto(ticker, _market);
                            return $"{sid},{ticker},{string.Join(",", x.Value)}";
                        })
                        .ToList();
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
        /// Helper method to get USD dolar volume from quote currency-intermittent pair
        /// </summary>
        /// <param name="coarseByDate">The data sorted by date</param>
        /// <param name="baseCurrency">Reference hash table for mapping quote currency</param>
        /// <param name="dataTicker">The ticker needs to get exchange rate to USD</param>
        /// <return>
        /// Dollar Volume in USD
        /// </return>
        public decimal? GetUSDDollarVolume(Dictionary<string, List<decimal?>> coarseByDate, Dictionary<string, string> baseCurrency, string dataTicker)
        {
            decimal? usdDollorVol;

            // To USD dollar volume conversion, if <ticker>-USD/BUSD/USDT/USDC/BTC pair exist
            if (dataTicker.Substring(dataTicker.Length - 3) != "USD")
            {
                string refCurrency;
                decimal? rateToUSD = 1m;

                if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}USD"))
                {
                    refCurrency = $"{baseCurrency[dataTicker]}USD";
                }
                else if (coarseByDate.ContainsKey($"USD{baseCurrency[dataTicker]}"))
                {
                    refCurrency = $"USD{baseCurrency[dataTicker]}";
                }
                else if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}BUSD"))
                {
                    refCurrency = $"{baseCurrency[dataTicker]}BUSD";
                }
                else if (coarseByDate.ContainsKey($"BUSD{baseCurrency[dataTicker]}"))
                {
                    refCurrency = $"BUSD{baseCurrency[dataTicker]}";
                }
                else if (coarseByDate.ContainsKey($"USDT{baseCurrency[dataTicker]}"))
                {
                    rateToUSD = coarseByDate.ContainsKey("USDTUSD") ? 
                        coarseByDate["USDTUSD"].First() :
                        1m / coarseByDate["BUSDUSDT"].First();
                    refCurrency = $"USDT{baseCurrency[dataTicker]}";
                }
                else if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}USDC"))
                {
                    rateToUSD = coarseByDate.ContainsKey("USDCUSD") ? 
                        coarseByDate["USDCUSD"].First() :
                        coarseByDate["USDCBUSD"].First();
                    refCurrency = $"{baseCurrency[dataTicker]}USDC";
                }
                else if (coarseByDate.ContainsKey($"{baseCurrency[dataTicker]}BTC"))
                {
                    rateToUSD = coarseByDate.ContainsKey("BTCUSD") ? 
                        coarseByDate["BTCUSD"].First() :
                        coarseByDate["BTCBUSD"].First();
                    refCurrency = $"{baseCurrency[dataTicker]}BTC";
                }
                else
                {
                    return null;
                }

                var conversionRate = coarseByDate[refCurrency].First();
                conversionRate = refCurrency.Substring(0, 3) == "USD" || 
                    refCurrency.Substring(0, 4) == "BUSD" ? 
                    1m / conversionRate :       // that would mean USD is the target currency not base currency
                    conversionRate;
                var baseDollarVol = coarseByDate[dataTicker][2];
                usdDollorVol = baseDollarVol * conversionRate * rateToUSD;
            }
            else
            {
                usdDollorVol = coarseByDate[dataTicker][2];
            }

            return usdDollorVol;
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