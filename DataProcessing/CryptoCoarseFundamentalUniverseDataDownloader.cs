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
using System.IO;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.DataSource;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Securities.CurrencyConversion;
using QuantConnect.ToolBox;
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

        private Dictionary<string, string> _baseCurrency = new();

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
                var symbolProperties = Extensions.DownloadData("https://raw.githubusercontent.com/QuantConnect/Lean/master/Data/symbol-properties/symbol-properties-database.csv")
                    .Split("\n");
                foreach (var line in symbolProperties)
                {
                    var lineItem = line.Split(",");
                    if (lineItem.First() == _market)
                    {
                        CurrencyPairUtil.DecomposeCurrencyPair(Symbol.Create(lineItem[1], SecurityType.Crypto, _market), out var _, out var quoteCurrency);
                        _baseCurrency[lineItem[1]] = quoteCurrency;
                    }
                }

                // Get all trade data files in set crypto market data file
                foreach(var file in Directory.GetFiles(Path.Combine(_baseFolder, "daily"), "*_trade.zip", SearchOption.AllDirectories))
                {
                    var dataReader = new LeanDataReader(file);
                    var baseData = dataReader.Parse();

                    var fileInfo = new FileInfo(file);
                    LeanData.TryParsePath(fileInfo.FullName, out var symbol, out _, out _);

                    foreach (TradeBar data in baseData)
                    {
                        var dateTime = data.EndTime;
                        if (dateTime < _fromDate || dateTime > _toDate)
                        {
                            continue;
                        }

                        var date = $"{dateTime:yyyyMMdd}";
                        
                        if (!_dataByDate.TryGetValue(date, out var content))
                        {
                            _dataByDate[date] = content = new Dictionary<string, List<decimal?>>();
                        }
                        
                        content[symbol.ID.Symbol] = new List<decimal?>{data.Open, data.High, data.Low, data.Close, data.Volume};
                    }
                }

                foreach (var date in _dataByDate.Keys.ToList())
                {
                    var coarseByDate = _dataByDate[date];

                    foreach (var dataTicker in coarseByDate.Keys.ToList())
                    {
                        decimal? usdVol = null;

                        // In case there might be missing data
                        try
                        {
                            usdVol = GetUSDVolume(date, dataTicker);
                        }
                        catch
                        {
                            Log.Trace($"No USD-{dataTicker} rate conversion available on {date}.");
                        }

                        coarseByDate[dataTicker].Add(usdVol);
                    }

                    // Save to file
                    var fileContent = coarseByDate.Select(x => 
                        {
                            var ticker = x.Key;
                            var sid = SecurityIdentifier.GenerateCrypto(ticker, _market);
                            return $"{sid},{ticker},{string.Join(",", x.Value)}";
                        })
                        .ToList();
                    var finalPath = Path.Combine(_destinationFolder, $"{date}.csv");
                    var finalFileExists = File.Exists(finalPath);

                    var lines = new HashSet<string>(fileContent);
                    if (finalFileExists)
                    {
                        foreach (var line in File.ReadAllLines(finalPath))
                        {
                            lines.Add(line);
                        }
                    }

                    var finalLines = lines.OrderBy(x => x.Split(',').First());
                    File.WriteAllLines(finalPath, finalLines);
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
        /// Helper method to get USD volume from quote currency-intermittent pair
        /// </summary>
        /// <param name="date">Date of data</param>
        /// <param name="ticker">The ticker needs to get exchange rate to USD</param>
        /// <return>
        /// Volume in USD
        /// </return>
        public decimal? GetUSDVolume(string date, string ticker)
        {
            decimal? usdVol = null;
            var dict = _dataByDate[date];
            var data = dict[ticker];
            var baseVolume = data[^1];

            // To USD volume conversion, if <ticker>-BTC/USD/BUSD/USDC/USDP/USDT pair exist
            if (ticker.Substring(ticker.Length - 3) != "USD")
            {
                var quoteCurrency = _baseCurrency[ticker];
                var targetCurrency = _market == Market.Binance ? "BUSD" : "USD";

                var existingSecurities = new List<Security>
                {
                    CreateSecurity(Symbol.Create(ticker, SecurityType.Crypto, _market))
                };
                existingSecurities[0].SetMarketPrice(new Tick { Value = (decimal)data[^2] });
                
                foreach(var cur in new List<string>{targetCurrency, "BTC", "USDC", "USDP", "USDT"})
                {
                    foreach(var intermediateTicker in new List<string>{$"{cur}{quoteCurrency}", $"{quoteCurrency}{cur}", $"{cur}{targetCurrency}", $"{targetCurrency}{cur}"})
                    {
                        if (dict.ContainsKey(intermediateTicker))
                        {
                            var symbol = Symbol.Create(intermediateTicker, SecurityType.Crypto, _market);
                            var sec = CreateSecurity(symbol);
                            existingSecurities.Add(sec);
                            sec.SetMarketPrice(new Tick { Value = (decimal)dict[symbol.ID.Symbol][^2] });
                        } 
                    }
                };

                var currencyConversion = SecurityCurrencyConversion.LinearSearch(
                    quoteCurrency,
                    targetCurrency,
                    existingSecurities,
                    new List<Symbol>(),
                    CreateSecurity);
                var conversionRate = currencyConversion.Update();

                usdVol = baseVolume * conversionRate;
            }
            else
            {
                usdVol = baseVolume;
            }

            return usdVol;
        }

        /// <summary>
        /// Helper method to create security for conversion.
        /// </summary>
        /// <param name="symbol">symbol to be added to Securities</param>
        private static Security CreateSecurity(Symbol symbol)
        {
            var timezone = TimeZones.Utc;

            var config = new SubscriptionDataConfig(
                typeof(TradeBar),
                symbol,
                Resolution.Daily,
                timezone,
                timezone,
                true,
                false,
                true);

            return new Security(
                SecurityExchangeHours.AlwaysOpen(timezone),
                config,
                new Cash(Currencies.USD, 0, 1),
                SymbolProperties.GetDefault(Currencies.USD),
                ErrorCurrencyConverter.Instance,
                RegisteredSecurityDataTypesProvider.Null,
                new SecurityCache()
            );
        }
    }
}