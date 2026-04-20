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
    

    def initialize(self):
        ''' Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized. '''

        self.set_start_date(2020, 6, 1)
        self.set_end_date(2020, 6, 5)
        self.set_cash(100000)
        
        self.set_brokerage_model(BrokerageName.BITFINEX, AccountType.CASH)
        
        # Warm up the security with the last known price to avoid conversion error
        self.set_security_initializer(lambda security: security.set_market_price(self.get_last_known_price(security)));

        # Data ADDED via universe selection is added with Daily resolution.
        self.universe_settings.resolution = Resolution.DAILY
        # Add universe selection of cryptos based on coarse fundamentals
        self.add_universe_selection(CoinbaseCoarseFundamentalUniverseSelectionModel(self.universe_settings))

        # Alternatively, we can define the selector function in the algorithm class:
        #self.add_universe_selection(CryptoCoarseFundamentalUniverseSelectionModel(Market.GDAX, self.universe_settings, self._universe_selection_filter))
        

    def _universe_selection_filter(self, crypto_coarse):
        ''' Selected the securities
        
        :param List of CryptoCoarseFundamentalUniverse crypto_coarse: List of CryptoCoarseFundamentalUniverse
        :return: List of Symbol objects '''
        filtered = [datum for datum in crypto_coarse
                if datum.volume >= 100 
                and datum.volume_in_usd > 10000]
        sorted_by_volume_in_usd = sorted(filtered, key=lambda datum: datum.volume_in_usd, reverse=True)[:10]
        
        return [datum.symbol for datum in sorted_by_volume_in_usd]

    def on_securities_changed(self, changes):
        ''' Event fired each time that we add/remove securities from the data feed
		
        :param SecurityChanges changes: Security additions/removals for this time step
        '''
        self.log(str(changes))


class CoinbaseCoarseFundamentalUniverseSelectionModel(CryptoCoarseFundamentalUniverseSelectionModel):

    def __init__(self, universe_settings):
        def selector(crypto_coarse):
            filtered = [datum for datum in crypto_coarse
                if datum.volume >= 100 
                and datum.volume_in_usd > 10000]
            sorted_by_volume_in_usd = sorted(filtered, key=lambda datum: datum.volume_in_usd, reverse=True)[:10]
            return [datum.symbol for datum in sorted_by_volume_in_usd]
        super().__init__(Market.GDAX, universe_settings, selector)