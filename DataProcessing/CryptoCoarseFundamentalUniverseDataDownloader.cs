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
using QuantConnect.Configuration;
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
        private readonly string _baseFolder;
        private readonly string _destinationFolder;

        private Dictionary<Symbol, string> _quoteCurrency = new();
        private SortedDictionary<string, Dictionary<Symbol, List<decimal?>>> _dataByDate = new();
        private Dictionary<Symbol, Security> _existingSecurities = new();

        /// <summary>
        /// Creates a new instance of <see cref="CryptoCoarseFundamental"/>
        /// </summary>
        /// <param name="market">The folder where the base data saved at</param>
        public CryptoCoarseFundamentalUniverseDataConverter(string market)
        {
            _baseFolder = Path.Combine(Globals.DataFolder, "crypto", market, "daily");
            _destinationFolder = Path.Combine(Config.Get("temp-output-directory", "/temp-output-directory"),
                "crypto", market, "fundamental", "coarse");

            Directory.CreateDirectory(_destinationFolder);
        }

        /// <summary>
        /// Load the local base trade data files, convert into universe file
        /// </summary>
        public bool ConvertToUniverseFile()
        {
            var start = DateTime.UtcNow;
            try
            {
                // Get all trade data files in set crypto market data file
                foreach(var file in Directory.GetFiles(_baseFolder, "*_trade.zip", SearchOption.AllDirectories))
                {
                    var fileInfo = new FileInfo(file);
                    LeanData.TryParsePath(fileInfo.FullName, out var symbol, out _, out _);

                    if (!SymbolPropertiesDatabase.FromDataFolder().ContainsKey(symbol.ID.Market, symbol, symbol.SecurityType))
                    {
                        Log.Trace($"CryptoCoarseFundamentalUniverseDataConverter.ConvertToUniverseFile(): ignoring symbol {symbol}");
                        continue;
                    }

                    try
                    {
                        CurrencyPairUtil.DecomposeCurrencyPair(symbol, out _, out var quoteCurrency);
                        _quoteCurrency[symbol] = quoteCurrency;

                        _existingSecurities.Add(symbol, CreateSecurity(symbol, quoteCurrency));
                    }
                    catch
                    {
                        // pass
                    }

                    var dataReader = new LeanDataReader(file);
                    var baseData = dataReader.Parse();
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

                var errorsPerSymbol = new Dictionary<Symbol, List<string>>();
                foreach (var dataByDate in _dataByDate)
                {
                    var date = dataByDate.Key;
                    var coarseByDate = dataByDate.Value;

                    // Update all securities daily price for conversion
                    foreach (var security in _existingSecurities.Values)
                    {
                        if (coarseByDate.TryGetValue(security.Symbol, out var data))
                        {
                            security.SetMarketPrice(new Tick { Value = (decimal)data[^2] });
                        }
                        else
                        {
                            // if the security doesn't have price for this date clear it
                            security.Cache.Reset();
                        }
                    }

                    foreach (var kvp in coarseByDate)
                    {
                        var dataSymbol = kvp.Key;
                        var content = kvp.Value;

                        decimal? usdVol = null;
                        
                        // In case there might be missing data
                        try
                        {
                            var volume = content[^1];
                            var rawUsdVol = GetUSDVolume(volume, _quoteCurrency[dataSymbol], _existingSecurities.Values.Where(security => security.Price != 0).ToList());
                            usdVol = rawUsdVol == null? null : Extensions.SmartRounding((decimal)rawUsdVol);
                        }
                        catch
                        {
                            if (!errorsPerSymbol.TryGetValue(dataSymbol, out var errors))
                            {
                                errorsPerSymbol[dataSymbol] = errors = new List<string>();
                            }
                            errors.Add(date);
                        }

                        content.Add(usdVol);
                    }

                    // Save to file
                    var fileContent = coarseByDate.Select(x => 
                        {
                            var sid = x.Key.ID;
                            return $"{sid},{sid.Symbol},{string.Join(",", x.Value)}";
                        })
                        .OrderBy(x => x.Split(',').First())
                        .ToHashSet();
                    var finalPath = Path.Combine(_destinationFolder, $"{date}.csv");

                    File.WriteAllLines(finalPath, fileContent);

                    Log.Trace($"CryptoCoarseFundamentalUniverseDataConverter.ConvertToUniverseFile(): processed {date}");
                }

                foreach (var errors in errorsPerSymbol)
                {
                    Log.Trace("CryptoCoarseFundamentalUniverseDataConverter.ConvertToUniverseFile(): " +
                              $"No USD-{errors.Key.ID.Symbol} rate conversion available on: [{string.Join(",", errors.Value)}].");
                }
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"CryptoCoarseFundamentalUniverseDataConverter.ConvertToUniverseFile(): Finished Processing. Took: {DateTime.UtcNow - start}");
            return true;
        }

        /// <summary>
        /// Helper method to get USD volume from quote currency-intermittent pair
        /// </summary>
        /// <param name="baseVolume">The volume of symbol at date in quote currency</param>
        /// <param name="quoteCurrency">The quote currency of the Symbol</param>
        /// <param name="existingSecurities">List of existing securities available to exchange rate</param>
        /// <return>
        /// Volume in USD
        /// </return>
        public static decimal? GetUSDVolume(
            decimal? baseVolume,
            string quoteCurrency,
            List<Security> existingSecurities
        )
        {
            var currencyConversion = SecurityCurrencyConversion.LinearSearch(
                quoteCurrency,
                Currencies.USD,
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
            return new Security(symbol,
                SecurityExchangeHours.AlwaysOpen(TimeZones.Utc),
                new Cash(quoteCurrency, 0, 0),
                SymbolProperties.GetDefault(quoteCurrency),
                ErrorCurrencyConverter.Instance,
                RegisteredSecurityDataTypesProvider.Null,
                new SecurityCache()
            );
        }
    }
}