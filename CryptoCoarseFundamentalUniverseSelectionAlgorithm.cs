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
using QuantConnect.Data;
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
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

            SetStartDate(2017, 2, 14);
            SetEndDate(2017, 2, 18);
            SetCash(100000);

            SetBrokerageModel(BrokerageName.Binance, AccountType.Cash);

            // Add universe selection of cryptos based on coarse fundamentals
            AddUniverse(new CryptoCoarseFundamentalUniverse(Market.Binance, UniverseSettings, UniverseSelectionFilter));
        }

        private IEnumerable<Symbol> UniverseSelectionFilter(IEnumerable<CryptoCoarseFundamental> data)
        {
            return from datum in data
                where datum.Volume >= 100m && datum.DollarVolume > 10000m
                select datum.Symbol;
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