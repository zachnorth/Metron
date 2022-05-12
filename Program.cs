
using System.Text;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.ComponentModel;

using Azure.Data.Tables;
using Azure;

// Library to use for plotting
// using ScottPlot;

namespace BlobQuickstartV12
{
    class NewTableData : ITableEntity
    {

        public NewTableData(string PartitionKey_, string RowKey_)
        {
            PartitionKey = PartitionKey_;
            RowKey = RowKey_;
        }

        public NewTableData(
            int addr32,
            int dataLen,
            byte commandResponseByte,
            int customerID,
            sbyte rssi,
            byte[] dayOfLogs,
            short crc,
            long currentRead,
            ulong spare64,
            ushort logLen,
            short errorFlags,
            ushort hardwareVersion,
            ushort spare16,
            short batteryVoltage,
            ushort avgDailyCurrent,
            byte rssi_1,
            byte noConsumption,
            byte activeFunctions,
            sbyte temDegreeF_AVG,
            sbyte tempDegreeF_MIN,
            sbyte tempDegreeF_MAX,
            byte decimalPosition,
            byte day,
            byte month,
            byte year,
            byte hour,
            byte minute,
            byte second,
            byte spare8_0,
            char dispUnit,
            byte meterModel,
            byte[] fwVersionNumber,
            byte[] meterId,
            byte signalQuality,
            byte spare8_1)
        {
            Addr32 = addr32;
            DataLen = dataLen;
            CommandResponseByte = commandResponseByte;
            CustomerID = customerID;
            RSSI = rssi;
            DayOfLogs = dayOfLogs;
            CRC = crc;
            CurrentRead = currentRead;
            Spare64 = spare64;
            LogLen = logLen;
            ErrorFlags = errorFlags;
            HardwareVersion = hardwareVersion;
            Spare16 = spare16;
            BatteryVoltage = batteryVoltage;
            AvgDailyCurrent = avgDailyCurrent;
            RSSI_1 = rssi_1;
            NoConsumption = noConsumption;
            ActiveFunctions = activeFunctions;
            TemDegreeF_AVG = temDegreeF_AVG;
            TempDegreeF_MIN = tempDegreeF_MIN;
            TempDegreeF_MAX = tempDegreeF_MAX;
            DecimalPosition = decimalPosition;
            Day = day;
            Month = month;
            Year = year;
            Hour = hour;
            Minute = minute;
            Second = second;
            Spare8_0 = spare8_0;
            DispUnit = dispUnit;
            MeterModel = meterModel;
            FwVersionNumber = fwVersionNumber;
            MeterId = meterId;
            SignalQuality = signalQuality;
            Spare8_1 = spare8_1;
        }

        
        public string PartitionKey { get; set ; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        // Mine
        public int Addr32 { get; set ; } = 0;
        public int DataLen { get; set ; } = 0;
        public byte CommandResponseByte { get; set ; }
        public int CustomerID { get; set ; } = 0;
        public sbyte RSSI { get; set ; }
        public byte[] DayOfLogs { get; set; } = { 0 };
        public short CRC { get; set ; }

        public long CurrentRead { get; set ; } = 0;
        public ulong Spare64 { get; set ; } = 0;
        public ushort LogLen { get; set ; } = 0;
        public short ErrorFlags { get; set ; } = 0;
        public ushort HardwareVersion { get; set ; } = 0;
        public ushort Spare16 { get; set ; } = 0;
        public short BatteryVoltage { get; set ; } = 0;
        public ushort AvgDailyCurrent { get; set ; } = 0;
        public byte RSSI_1 { get; set; } = 0;
        public byte NoConsumption { get; set ; } = 0;
        public byte ActiveFunctions { get; set ; } = 0;
        public sbyte TemDegreeF_AVG { get; set ; } = 0;
        public sbyte TempDegreeF_MIN { get; set ; } = 0;
        public sbyte TempDegreeF_MAX { get; set ; } = 0;
        public byte DecimalPosition { get; set ; } = 0;
        public byte Day { get; set ; } = 0;
        public byte Month { get; set ; } = 0;
        public byte Year { get; set ; } = 0;
        public byte Hour { get; set ; } = 0;
        public byte Minute { get; set ; } = 0;
        public byte Second { get; set ; } = 0;
        public byte Spare8_0 { get; set ; } = 0;
        public char DispUnit { get; set; } = "M"[0];
        public byte MeterModel { get; set ; } = 0;
        public byte[] FwVersionNumber { get; set ; } = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11 };
        public byte[] MeterId { get; set ; } = { 0, 1, 2, 3, 4, 5, 6, 7 };
        public byte SignalQuality { get; set ; } = 0;
        public byte Spare8_1 { get; set ; } = 0;
    }


    class Program
    {
        static async Task Main()
        {
            string tableName = "testInputData";
            string newTableName = "newTable";
            string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");

            // Create clients for both tables using Azure Connection String
            TableClient tableClient = new TableClient(connectionString, tableName);
            TableClient newTable = new TableClient(connectionString, newTableName);



            // Query testInputData table for data
            Pageable<TableEntity> results = tableClient.Query<TableEntity>(filter: $"", maxPerPage: 10);

            // Get first row from table
            TableEntity currentTableData = results.First<TableEntity>();


            string temp = currentTableData.GetString("InputData");

            // Parse binary data into NewTableData structure
            NewTableData newTableData = ParseBinaryData_1(ConvertToByteArray(currentTableData.GetString("InputData")));

            // Print each property of the NewTableData object
            foreach(PropertyDescriptor descriptor in TypeDescriptor.GetProperties(newTableData))
            {
                string name = descriptor.Name;
                object value = descriptor.GetValue(newTableData);
                Console.WriteLine("{0}={1}", name, value);
            }


            // Attempt to plot data but not enough time to work through it

            // Plot the log data
            // double[] TimeStamps = {};
            // for(int i = 0; i < 48; i++)
            // {
            //     TimeStamps.Append(i * 0.5);
            // }

            // PlotData(newTableData.DayOfLogs, TimeStamps);


            // Use partition key and row key from testInputData table
            newTableData.PartitionKey = currentTableData.GetString("PartitionKey");
            newTableData.RowKey = currentTableData.GetString("RowKey");

            // Create new table if it does not already exist
            var response = newTable.CreateIfNotExists();

            // Asynchronously add new entry to table with parsed data
            await newTable.AddEntityAsync(newTableData);
        }

        // Takes in a string of bytes and converts
        // it into a list of those bytes
        public static List<byte> ConvertToByteArray(string str)
        {
            return Enumerable.Range(0, str.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(str.Substring(x, 2), 16))
                             .ToList<byte>();
        }


        // Doesn't serialize data into class properly
        public static NewTableData DeserializeData(TableEntity entity, byte[] binaryData)
        {
            IFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream(binaryData);
            BinaryReader binaryReader = new BinaryReader(stream, Encoding.ASCII);
            

            NewTableData newTableData = (NewTableData)formatter.Deserialize(stream);
            newTableData.PartitionKey = entity.GetString("PartitionKey");
            newTableData.PartitionKey = entity.GetString("RowKey");


            return newTableData;
        }


        // Doesn't provide extra functionality but cleans up code
        // Takes in a List of bytes and a function(ex: int.Parse), 
        // converts it to a hex string and the calls the T.Parse 
        // function (ex: int.Parse) to parse the hex string into 
        // the correct data type and returns it.
        public static T ConverData<T>(List<byte> data, Func<string, System.Globalization.NumberStyles, T> f)
        {
            T retVal = f(Convert.ToHexString(data.ToArray()), System.Globalization.NumberStyles.HexNumber);
            return retVal;
        }


        // Parses the binary data into a hex string
        // then converts the hexstring into the correct
        // data type. Returns NewDataTable object with 
        // all data fields assigned.
        public static NewTableData ParseBinaryData_1(List<byte> binaryData)
        {
            int Addr32 =                ConverData<int>(binaryData.GetRange(0, 4), int.Parse);
            ushort DataLen =            ConverData<ushort>(binaryData.GetRange(4, 2), ushort.Parse); 
            byte CommandResponseByte =  ConverData<byte>(binaryData.GetRange(6, 1), byte.Parse);
            ushort CustomerID =         ConverData<ushort>(binaryData.GetRange(7, 2), ushort.Parse);
            sbyte RSSI =                ConverData<sbyte>(binaryData.GetRange(9, 1), sbyte.Parse);
            long CurrentRead =          ConverData<long>(binaryData.GetRange(10, 8), long.Parse);
            ulong Spare64 =             ConverData<ulong>(binaryData.GetRange(18, 8), ulong.Parse);
            ushort LogLen =             ConverData<ushort>(binaryData.GetRange(26, 2), ushort.Parse);
            short ErrorFlags =          ConverData<short>(binaryData.GetRange(28, 2), short.Parse);
            ushort HardwareVersion =    ConverData<ushort>(binaryData.GetRange(30, 2), ushort.Parse);
            ushort Spare16 =            ConverData<ushort>(binaryData.GetRange(32, 2), ushort.Parse);
            short BatteryVoltage =      ConverData<short>(binaryData.GetRange(34, 2), short.Parse);
            ushort AvgDailyCurrent =    ConverData<ushort>(binaryData.GetRange(36, 2), ushort.Parse);
            byte RSSI_1 =               ConverData<byte>(binaryData.GetRange(38, 1), byte.Parse);
            byte NoConsumption =        ConverData<byte>(binaryData.GetRange(39, 1), byte.Parse);
            byte ActiveFunctions =      ConverData<byte>(binaryData.GetRange(40, 1), byte.Parse);
            sbyte TemDegreeF_AVG =      ConverData<sbyte>(binaryData.GetRange(41, 1), sbyte.Parse);
            sbyte TempDegreeF_MIN =     ConverData<sbyte>(binaryData.GetRange(42, 1), sbyte.Parse);
            sbyte TempDegreeF_MAX =     ConverData<sbyte>(binaryData.GetRange(43, 1), sbyte.Parse);
            byte DecimalPosition =      ConverData<byte>(binaryData.GetRange(44, 1), byte.Parse);
            byte Day =                  ConverData<byte>(binaryData.GetRange(45, 1), byte.Parse);
            byte Month =                ConverData<byte>(binaryData.GetRange(46, 1), byte.Parse);
            byte Year =                 ConverData<byte>(binaryData.GetRange(47, 1), byte.Parse);
            byte Hour =                 ConverData<byte>(binaryData.GetRange(48, 1), byte.Parse);
            byte Minute =               ConverData<byte>(binaryData.GetRange(49, 1), byte.Parse);
            byte Second =               ConverData<byte>(binaryData.GetRange(50, 1), byte.Parse);
            byte Spare8_0 =             ConverData<byte>(binaryData.GetRange(51, 1), byte.Parse);
            char DispUnit =      (char) ConverData<byte>(binaryData.GetRange(52, 1), byte.Parse);
            byte MeterModel =           ConverData<byte>(binaryData.GetRange(53, 1), byte.Parse);

            byte[] FwVersionNumber = new byte[12];
            for(int i = 0; i < 12; i++)
            {
                FwVersionNumber[i] =  ConverData<byte>(binaryData.GetRange(54 + i, 1), byte.Parse);
            }

            byte[] MeterId = new byte[8];
            for(int i = 0; i < 8; i++)
            {
                MeterId[i] = ConverData<byte>(binaryData.GetRange(66 + i, 1), byte.Parse);
            }

            byte SignalQuality =    ConverData<byte>(binaryData.GetRange(74, 1), byte.Parse);
            byte Spare8_1 =         ConverData<byte>(binaryData.GetRange(75, 1), byte.Parse);

            byte[] DayOfLogs = new byte[192];
            for(int i = 0; i < 185; i++)
            {
                DayOfLogs[i] = ConverData<byte>(binaryData.GetRange(76 + i, 1), byte.Parse);
            }

            short CRC = ConverData<short>(binaryData.GetRange(261, 2), short.Parse);

            return new NewTableData(
                Addr32,
                DataLen,
                CommandResponseByte,
                CustomerID,
                RSSI,
                DayOfLogs,
                CRC,
                CurrentRead,
                Spare64,
                LogLen,
                ErrorFlags,
                HardwareVersion,
                Spare16,
                BatteryVoltage,
                AvgDailyCurrent,
                RSSI_1,
                NoConsumption,
                ActiveFunctions,
                TemDegreeF_AVG,
                TempDegreeF_MIN,
                TempDegreeF_MAX,
                DecimalPosition,
                Day,
                Month,
                Year,
                Hour,
                Minute,
                Second,
                Spare8_0,
                DispUnit,
                MeterModel,
                FwVersionNumber,
                MeterId,
                SignalQuality,
                Spare8_1
            );
        }
    
        // Attempt to plot some data but
        // not enough time to work through it
        public static void PlotData(int[] dataX, double[] dataY)
        {
            // var plt = new ScottPlot.Plot(400, 300);
            // plt.AddScatter(dataX, dataY);
            // plt.SaveFig("quickstart.png");
        }
    }
}