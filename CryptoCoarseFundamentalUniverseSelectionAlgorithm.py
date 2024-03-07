# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

### <summary>
### Example algorithm using the custom data type as a source of alpha
### </summary>
class CryptoCoarseFundamentalUniverseSelectionAlgorithm(QCAlgorithm):
    def Initialize(self):
        ''' Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized. '''

        self.SetStartDate(2020, 6, 1)
        self.SetEndDate(2020, 6, 5)
        self.SetCash(100000)
        
        self.SetBrokerageModel(BrokerageName.Bitfinex, AccountType.Cash)
        
        # Warm up the security with the last known price to avoid conversion error
        self.SetSecurityInitializer(lambda security: security.SetMarketPrice(self.GetLastKnownPrice(security)));

        # Data ADDED via universe selection is added with Daily resolution.
        self.UniverseSettings.Resolution = Resolution.Daily
        # Add universe selection of cryptos based on coarse fundamentals
        universe = self.AddUniverse(CryptoUniverseConfig(Market.Bitfinex, self.UniverseSettings, self.UniverseSelectionFilter))

        history = self.History(universe, TimeSpan(2, 0, 0, 0))
        if len(history) != 2:
            raise ValueError(f"Unexpected history count {len(history)}! Expected 2")

        for dataForDate in history:
            if len(dataForDate) < 100:
                raise ValueError(f"Unexpected historical universe data!")

    def UniverseSelectionFilter(self, data):
        ''' Selected the securities
        
        :param List of CryptoUniverseConfig data: List of CryptoUniverseConfig
        :return: List of Symbol objects '''
        filtered = [datum for datum in data
                if datum.Volume >= 100 
                and datum.VolumeInUsd > 10000]
        sorted_by_volume_in_usd = sorted(filtered, key=lambda datum: datum.VolumeInUsd, reverse=True)[:10]
        
        return [datum.Symbol for datum in sorted_by_volume_in_usd]

    def OnSecuritiesChanged(self, changes):
        ''' Event fired each time that we add/remove securities from the data feed

        :param SecurityChanges changes: Security additions/removals for this time step
        '''
        self.Log(changes.ToString())