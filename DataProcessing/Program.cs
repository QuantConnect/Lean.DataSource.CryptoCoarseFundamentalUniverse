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
using System.IO;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Util;
using QuantConnect.ToolBox;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Entrypoint for the data converter
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Entrypoint of the program
        /// </summary>
        /// <returns>Exit code. 0 equals successful, and any other value indicates the converter failed.</returns>
        public static void Main(string[] args)
        {
            var optionsObject = ToolboxArgumentParser.ParseArguments(args);
            if (optionsObject.Count == 0)
            {
                Log.Error("DataProcessing.Program.Main(): No parameter was detected.");
                Environment.Exit(1);
            }

            if (!optionsObject.ContainsKey("market"))
            {
                throw new ArgumentException();
            }

            var market = optionsObject["market"].ToString();
            var fromDate = optionsObject.ContainsKey("from-date")
                ? Parse.DateTimeExact(optionsObject["from-date"].ToString(), "yyyy-MM-dd")
                : DateTime.Today;
            var toDate = optionsObject.ContainsKey("to-date")
                ? Parse.DateTimeExact(optionsObject["to-date"].ToString(), "yyyy-MM-dd")
                : DateTime.Today;

            // Get the config values first before running. These values are set for us
            // automatically to the value set on the website when defining this data type
            var baseFolder = Path.Combine(
                Globals.DataFolder,
                "crypto",
                market);

            CryptoCoarseFundamentalUniverseDataConverter instance = null;
            try
            {
                // Pass in the values we got from the configuration into the converter.
                instance = new CryptoCoarseFundamentalUniverseDataConverter(market, baseFolder, fromDate, toDate);
            }
            catch (Exception err)
            {
                Log.Error(err, $"DataProcessing.Program.Main(): The converter for {market} Coarse Fundamental data failed to be constructed");
                Environment.Exit(1);
            }

            // No need to edit anything below here for most use cases.
            // The converter is ran and cleaned up for you safely here.
            try
            {
                // Run the data converter.
                var success = instance.ConvertToUniverseFile();
                if (!success)
                {
                    Log.Error($"DataProcessing.Program.Main(): Failed to process {market} Coarse Fundamental data");
                    Environment.Exit(1);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, $"DataProcessing.Program.Main(): The converter for {market} Coarse Fundamental data exited unexpectedly");
                Environment.Exit(1);
            }

            // The converter was successful
            Environment.Exit(0);
        }
    }
}