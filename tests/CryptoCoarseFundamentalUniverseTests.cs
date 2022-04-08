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
using ProtoBuf;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ProtoBuf.Meta;
using Newtonsoft.Json;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;
using QuantConnect.Data.Market;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class CryptoCoarseFundamentalTests
    {
        private static IEnumerable<object[]> TestCases()
        {
            yield return new object[] {Market.Kraken, new Dictionary<string, List<decimal?>>{{"BTCUSD", new List<decimal?>{100m,0.01m,1m}}}, new Dictionary<string, string>{{"BTCUSD","USD"}}, "BTCUSD", 100m};
            yield return new object[] {Market.GDAX, new Dictionary<string, List<decimal?>>{{"ETHBTC", new List<decimal?>{100m,0.1m,10m}}, {"BTCUSD", new List<decimal?>{10m,null,1m}}}, new Dictionary<string, string>{{"ETHBTC","BTC"}, {"BTCUSD","USD"}}, "ETHBTC", 10000m};
            yield return new object[] {Market.Bitfinex, new Dictionary<string, List<decimal?>>{{"BTCXRP", new List<decimal?>{100m,null,1m}}, {"USDTXRP", new List<decimal?>{10m,0.1m,1m}}, {"USDTUSD", new List<decimal?>{1m,1m,1m}}}, new Dictionary<string, string>{{"BTCXRP","XPR"}, {"USDTXRP","XRP"}, {"USDTUSD","USD"}}, "BTCXRP", 10m};
            yield return new object[] {Market.Binance, new Dictionary<string, List<decimal?>>{{"MATICBUSD", new List<decimal?>{100m,10m,1000m}}}, new Dictionary<string, string>{{"MATICBUSD","BUSD"}}, "MATICBUSD", 1000m};
            yield return new object[] {Market.FTX, new Dictionary<string, List<decimal?>>{{"SOLLTC", new List<decimal?>{1m,0.1m,0.1m}}, {"LTCBTC", new List<decimal?>{10m,0.1m,1m}}, {"BTCUSD", new List<decimal?>{10m,0m,0m}}}, new Dictionary<string, string>{{"SOLLTC","LTC"}, {"LTCBTC","BTC"}, {"BTCUSD","USD"}}, "SOLLTC", 10m};
        }

        [Test]
        public void JsonRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();
            var serialized = JsonConvert.SerializeObject(expected);
            var result = JsonConvert.DeserializeObject(serialized, type);

            AssertAreEqual(expected, result);
        }

        [Test]
        public void Selection()
        {
            var datum = CreateNewSelection();

            var expected = from d in datum
                            where d.Price > 50m && d.VolumeInBaseCurrency > 10000m
                            select d.Symbol;
            var result = new List<Symbol> {Symbol.Create("ETHBUSD", SecurityType.Crypto, Market.Binance)};

            AssertAreEqual(expected, result);
        }

        [Test, TestCaseSource("TestCases")]
        public void GetUSDDollarVolumeTest(string market, Dictionary<string, List<decimal?>> coarseByDate, Dictionary<string, string> baseCurrency, string dataTicker, decimal expected)
        {
            // Convertor instance setting won't matter
            var convertor = new CryptoCoarseFundamentalUniverseDataConverter(market, Globals.DataFolder, new DateTime(2020, 1, 1), new DateTime(2020, 2, 1));

            var result = convertor.GetUSDDollarVolume(coarseByDate, baseCurrency, dataTicker);

            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }

        private BaseData CreateNewInstance()
        {
            return new CryptoCoarseFundamental
                {
                    Volume = 20m,
                    VolumeInBaseCurrency = 200m,
                    DollarVolume = 200m,
                    Open = 5m,
                    High = 15m,
                    Low = 4m,
                    Close = 10m,

                    Symbol = Symbol.Create("BTCBUSD", SecurityType.Crypto, Market.Binance),
                    Time = DateTime.Today
                };
        }

        private IEnumerable<CryptoCoarseFundamental> CreateNewSelection()
        {
            return new []
            {
                new CryptoCoarseFundamental
                {
                    Volume = 20m,
                    VolumeInBaseCurrency = 200m,
                    DollarVolume = 200m,
                    Open = 5m,
                    High = 15m,
                    Low = 4m,
                    Close = 10m,

                    Symbol = Symbol.Create("BTCBUSD", SecurityType.Crypto, Market.Binance),
                    Time = DateTime.Today
                },
                new CryptoCoarseFundamental
                {
                    Volume = 200m,
                    VolumeInBaseCurrency = 20000m,
                    DollarVolume = 50000m,
                    Open = 50m,
                    High = 150m,
                    Low = 40m,
                    Close = 100m,

                    Symbol = Symbol.Create("ETHBUSD", SecurityType.Crypto, Market.Binance),
                    Time = DateTime.Today
                }
            };
        }
    }
}