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
using System.Linq;
using QuantConnect.Brokerages;
using QuantConnect.Securities;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// Example algorithm using the custom data type as a source of alpha
    /// </summary>
    public class CryptoCoarseFundamentalUniverseSelectionAlgorithm : QCAlgorithm
    {
        /// <summary>
        /// Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.
        /// </summary>
        public override void Initialize()
        {
            SetStartDate(2020, 6, 1);
            SetEndDate(2020, 6, 5);
            SetCash(100000);

            SetBrokerageModel(BrokerageName.Bitfinex, AccountType.Margin);

            // Warm up the security with the last known price to avoid conversion error
            SetSecurityInitializer(new BrokerageModelSecurityInitializer(BrokerageModel, new FuncSecuritySeeder(GetLastKnownPrices)));

            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;
            // Add universe selection of cryptos based on coarse fundamentals
            var universe = AddUniverse(CryptoUniverse.Bitfinex(UniverseSelectionFilter));

            var history = History(universe, 2).ToList();
            if (history.Count != 2)
            {
                throw new Exception($"Unexpected historical data count!");
            }
            foreach (var dataForDate in history)
            {
                var coarseData = dataForDate.ToList();
                if (coarseData.Count < 100)
                {
                    throw new Exception($"Unexpected historical universe data!");
                }
            }
        }

        private IEnumerable<Symbol> UniverseSelectionFilter(IEnumerable<CryptoUniverse> data)
        {
            return (from datum in data
                    where datum.Volume >= 100m && datum.VolumeInUsd > 10000m
                    orderby datum.VolumeInUsd descending
                    select datum.Symbol).Take(10);
        }

        /// <summary>
        /// Event fired each time that we add/remove securities from the data feed
        /// </summary>
        /// <param name="changes">Security additions/removals for this time step</param>
        public override void OnSecuritiesChanged(SecurityChanges changes)
        {
            Log(changes.ToString());
        }
    }
}