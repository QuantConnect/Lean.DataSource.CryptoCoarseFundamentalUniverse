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
 *
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using NodaTime;
using Python.Runtime;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;

namespace QuantConnect.DataSource
{
    [Obsolete("'CryptoCoarseFundamental' was renamed to 'CryptoUniverse'")]
    public class CryptoCoarseFundamental : CryptoUniverse {}

    /// <summary>
    /// Crypto Coarse Fundamental object for crpyto universe selection
    /// </summary>
    public class CryptoUniverse : BaseDataCollection
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);

       /// <summary>
        /// Daily Open Price (UTC 00:00)
        /// </summary>
        public decimal Open { get; set; }

        /// <summary>
        /// Daily High Price
        /// </summary>
        public decimal High { get; set; }

        /// <summary>
        /// Daily Low Price
        /// </summary>
        public decimal Low { get; set; }

        /// <summary>
        /// Daily Close Price
        /// </summary>
        public decimal Close { get; set; }

        /// <summary>
        /// Daily Trade Volume
        /// Note that this only includes the volume traded in the selected market
        /// </summary>
        public decimal Volume { get; set; }

        /// <summary>
        /// Daily Volume in Quote Currency
        /// Note that this only includes the volume traded in the selected market
        /// </summary>
        public decimal VolumeInQuoteCurrency { get; set; }

        /// <summary>
        /// Daily Volume in USD
        /// Note that this only includes the volume traded in the selected market
        /// </summary>
        public decimal? VolumeInUsd { get; set; }

        /// <summary>
        /// Alias of close price
        /// </summary>
        public decimal Price => Close;

         /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "crypto",
                    config.Market.ToLowerInvariant(),
                    "fundamental",
                    "coarse",
                    $"{date:yyyyMMdd}.csv"
                ),
                SubscriptionTransportMedium.LocalFile,
                FileFormat.FoldingCollection
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');

            return new CryptoUniverse
            {
                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date,

                Open = decimal.Parse(csv[2], NumberStyles.Any, CultureInfo.InvariantCulture),
                High = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture),
                Low = decimal.Parse(csv[4], NumberStyles.Any, CultureInfo.InvariantCulture),
                Close = decimal.Parse(csv[5], NumberStyles.Any, CultureInfo.InvariantCulture),
                Volume = decimal.Parse(csv[6], NumberStyles.Any, CultureInfo.InvariantCulture),
                VolumeInQuoteCurrency = Close * Volume,
                VolumeInUsd = !string.IsNullOrEmpty(csv[7]) ? 
                    decimal.Parse(csv[7], NumberStyles.Any, CultureInfo.InvariantCulture) :
                    null
            };
        }

        /// <summary>
        /// Creates the universe symbol
        /// </summary>
        /// <returns>A crypto coarse universe symbol</returns>
        public override Symbol UniverseSymbol(string market = null)
        {
            market ??= Market.Coinbase;
            var ticker = $"crypto-coarse-{Guid.NewGuid()}";
            var sid = SecurityIdentifier.GenerateCrypto(ticker, market);

            return new Symbol(sid, ticker);
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        public override BaseData Clone()
        {
            return new CryptoUniverse
            {
                Symbol = Symbol,
                Time = Time,
                EndTime = EndTime,
                Volume = Volume,
                VolumeInQuoteCurrency = VolumeInQuoteCurrency,
                VolumeInUsd = VolumeInUsd,
                Open = Open,
                High = High,
                Low = Low,
                Close = Close,
                Data = Data
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return false;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return false;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol},{Price},{Volume},{VolumeInQuoteCurrency},{VolumeInUsd},{Open},{High},{Low},{Close}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bitfinex(UniverseSettings universeSettings = null)
        {
            return Bitfinex(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bitfinex(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Bitfinex, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bitfinex(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Bitfinex, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Binance(UniverseSettings universeSettings = null)
        {
            return Binance(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Binance(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Binance, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Binance(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Binance, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bybit(UniverseSettings universeSettings = null)
        {
            return Bybit(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bybit(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Bybit, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Bybit(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Bybit, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Coinbase(UniverseSettings universeSettings = null)
        {
            return Coinbase(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Coinbase(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Coinbase, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Coinbase(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Coinbase, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe BinanceUS(UniverseSettings universeSettings = null)
        {
            return BinanceUS(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe BinanceUS(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.BinanceUS, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe BinanceUS(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.BinanceUS, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Kraken(UniverseSettings universeSettings = null)
        {
            return Kraken(universe => universe.Select(x => x.Symbol), universeSettings);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Kraken(Func<IEnumerable<CryptoUniverse>, IEnumerable<Symbol>> selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Kraken, universeSettings, selector);
        }

        /// <summary>
        /// Creates a new crypto universe
        /// </summary>
        /// <param name="selector">Returns the symbols that should be included in the universe</param>
        /// <param name="universeSettings">The settings used for new subscriptions generated by this universe</param>
        public static Universe Kraken(PyObject selector, UniverseSettings universeSettings = null)
        {
            return new CryptoUniverseFactory(Market.Kraken, universeSettings, selector);
        }
    }
}