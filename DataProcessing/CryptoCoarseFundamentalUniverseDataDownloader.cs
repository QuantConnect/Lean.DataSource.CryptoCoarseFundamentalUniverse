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
using QuantConnect;
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

        private Dictionary<Symbol, string> _quoteCurrency = new();
        private Dictionary<string, Dictionary<Symbol, List<decimal?>>> _dataByDate = new();
        private Dictionary<Symbol, Security> _existingSecurities = new();

        /// <summary>
        /// Creates a new instance of <see cref="CryptoCoarseFundamental"/>
        /// </summary>
        /// <param name="market">The data vendor market</param>
        /// <param name="baseFolder">The folder where the base data saved at</param>
        public CryptoCoarseFundamentalUniverseDataConverter(string market, string baseFolder)
        {
            _market = market;
            _baseFolder = baseFolder;
            _destinationFolder = Path.Combine(baseFolder, "fundamental", "coarse");

            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Load the local base trade data files, convert into universe file
        /// </summary>
        public bool ConvertToUniverseFile()
        {
            try
            {
                // Get all trade data files in set crypto market data file
                foreach(var file in Directory.GetFiles(Path.Combine(_baseFolder, "daily"), "*_trade.zip", SearchOption.AllDirectories))
                {
                    var dataReader = new LeanDataReader(file);
                    var baseData = dataReader.Parse();

                    var fileInfo = new FileInfo(file);
                    LeanData.TryParsePath(fileInfo.FullName, out var symbol, out _, out _);

                    CurrencyPairUtil.DecomposeCurrencyPair(symbol, out var _, out var quoteCurrency);
                    _quoteCurrency[symbol] = quoteCurrency;

                    _existingSecurities.Add(symbol, CreateSecurity(symbol, quoteCurrency));

                    foreach (TradeBar data in baseData)
                    {
                        var dateTime = data.EndTime;
                        var date = $"{dateTime:yyyyMMdd}";
                        
                        if (!_dataByDate.TryGetValue(date, out var content))
                        {
                            _dataByDate[date] = content = new Dictionary<Symbol, List<decimal?>>();
                        }
                        
                        content[symbol] = new List<decimal?>{data.Open, data.High, data.Low, data.Close, data.Volume};
                    }
                }

                foreach (var date in _dataByDate.Keys.ToList())
                {
                    var coarseByDate = _dataByDate[date];

                    foreach (var dataSymbol in coarseByDate.Keys.ToList())
                    {
                        _existingSecurities[dataSymbol].SetMarketPrice(new Tick { Value = (decimal)coarseByDate[dataSymbol][^2] });

                        decimal? usdVol = null;

                        // In case there might be missing data
                        try
                        {
                            var volume = _dataByDate[date][dataSymbol][^1];
                            usdVol = GetUSDVolume(volume, _market, _quoteCurrency[dataSymbol], _existingSecurities.Values.ToList());
                        }
                        catch
                        {
                            Log.Trace($"No USD-{dataSymbol.Value} rate conversion available on {date}.");
                        }

                        coarseByDate[dataSymbol].Add(Extensions.SmartRounding((decimal)usdVol));
                    }

                    // Save to file
                    var fileContent = coarseByDate.Select(x => 
                        {
                            var sid = x.Key.ID;
                            return $"{sid},{sid.Symbol},{string.Join(",", x.Value)}";
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
        /// <param name="baseVolume">The volume of symbol at date in quote currency</param>
        /// <param name="market">The market exchange</param>
        /// <param name="quoteCurrency">The quote currency of the Symbol</param>
        /// <param name="existingSecurities">List of existing securities available to exchange rate</param>
        /// <return>
        /// Volume in USD
        /// </return>
        public static decimal? GetUSDVolume(
            decimal? baseVolume,
            string market,
            string quoteCurrency,
            List<Security> existingSecurities
        )
        {
            var targetCurrency = market == Market.Binance ? "BUSD" : "USD";

            var currencyConversion = SecurityCurrencyConversion.LinearSearch(
                quoteCurrency,
                targetCurrency,
                existingSecurities,
                new List<Symbol>(),
                symbol => CreateSecurity(symbol, quoteCurrency));
            var conversionRate = currencyConversion.Update();

            decimal? usdVol = baseVolume * conversionRate;

            return usdVol;
        }

        /// <summary>
        /// Helper method to create security for conversion.
        /// </summary>
        /// <param name="symbol">Symbol to be added to Securities</param>
        /// <param name="quoteCurrency">The quote currency of the Symbol</param>
        public static Security CreateSecurity(Symbol symbol, string quoteCurrency)
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
                new Cash(quoteCurrency, 0, 1),
                SymbolProperties.GetDefault(quoteCurrency),
                ErrorCurrencyConverter.Instance,
                RegisteredSecurityDataTypesProvider.Null,
                new SecurityCache()
            );
        }
    }
}