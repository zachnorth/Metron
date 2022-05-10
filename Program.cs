using Azure.Data.Tables;
using Azure;

using System.Text;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using System.ComponentModel;
// using ScottPlot;

namespace BlobQuickstartV12
{

    [Serializable]
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
            int[] dayOfLogs,
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
            string dispUnit,
            byte meterModel,
            int[] fwVersionNumber,
            int[] meterId,
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
        public string CMND { get; set ; } = "CMND";
        public int[] DayOfLogs { get; set; } = { 0 };
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
        public string DispUnit { get; set; } = "M";
        public byte MeterModel { get; set ; } = 0;
        public int[] FwVersionNumber { get; set ; } = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11 };
        public int[] MeterId { get; set ; } = { 0, 1, 2, 3, 4, 5, 6, 7 };
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

            // Parse binary data into NewTableData structure
            NewTableData newTableData = ParseBinaryData(ConvertToByteArray(currentTableData.GetString("InputData"), Encoding.ASCII));

            // Print each property of the NewTableData object
            foreach(PropertyDescriptor descriptor in TypeDescriptor.GetProperties(newTableData))
            {
                string name = descriptor.Name;
                object value = descriptor.GetValue(newTableData);
                Console.WriteLine("{0}={1}", name, value);
            }

            Console.WriteLine("\n");
            for(int i = 0; i < newTableData.DayOfLogs.Length; i++)
            {
                Console.WriteLine("DayOfLog (" + i + "): " + newTableData.DayOfLogs[i]);
            }

            Console.WriteLine("\n");
            for(int i = 0; i < newTableData.FwVersionNumber.Length; i++)
            {
                Console.WriteLine("FwVersionNumber (" + i + "): " + newTableData.FwVersionNumber[i]);
            }

            Console.WriteLine("\n");
            for(int i = 0; i < newTableData.MeterId.Length; i++)
            {
                Console.WriteLine("MeterID (" + i + "): " + newTableData.MeterId[i]);
            }


            // Plot the log data
            double[] TimeStamps = {};
            for(int i = 0; i < 48; i++)
            {
                TimeStamps.Append(i * 0.5);
            }

            PlotData(newTableData.DayOfLogs, TimeStamps);


            // Use partition key and row key from testInputData table
            newTableData.PartitionKey = currentTableData.GetString("PartitionKey");
            newTableData.RowKey = currentTableData.GetString("RowKey");

            // Create new table if it does not already exist
            var response = newTable.CreateIfNotExists();

            // Asynchronously add new entry to table with parsed data
            await newTable.AddEntityAsync(newTableData);


        }

        // Takes in a string and an encoding and 
        // returns a byte array representing string
        // in specified encoding.
        public static byte[] ConvertToByteArray(string str, Encoding encoding)
        {
            return encoding.GetBytes(str);
        }

        public static NewTableData DeserializeData(TableEntity entity, byte[] binaryData)
        {
            IFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream(binaryData);
            BinaryReader binaryReader = new BinaryReader(stream);

            NewTableData data = new NewTableData(entity.GetString("PartitionKey"), entity.GetString("RowKey"));

            data = (NewTableData)formatter.Deserialize(stream);

            return data;
        }

        // Takes in byte array and parses it into
        // NewTableData class for inserting into new table
        public static NewTableData ParseBinaryData(byte[] binaryData)
        {
            MemoryStream stream = new MemoryStream(binaryData);
            BinaryReader binaryReader = new BinaryReader(stream);

            int Addr32 = binaryReader.ReadInt32();
            int DataLen = binaryReader.ReadInt16();
            byte CommandResponseByte = binaryReader.ReadByte();
            int CustomerID = binaryReader.ReadInt16();
            sbyte RSSI = binaryReader.ReadSByte();

            long CurrentRead = binaryReader.ReadInt64();
            ulong Spare64 = (ulong)binaryReader.ReadInt64();
            ushort LogLen = (ushort)binaryReader.ReadInt16();
            short ErrorFlags = binaryReader.ReadInt16();
            ushort HardwareVersion = binaryReader.ReadUInt16();
            ushort Spare16 = binaryReader.ReadUInt16();
            short BatteryVoltage = binaryReader.ReadInt16();
            ushort AvgDailyCurrent = binaryReader.ReadUInt16();
            byte RSSI_1 = binaryReader.ReadByte();
            byte NoConsumption = binaryReader.ReadByte();
            byte ActiveFunctions = binaryReader.ReadByte();
            sbyte TemDegreeF_AVG = binaryReader.ReadSByte();
            sbyte TempDegreeF_MIN = binaryReader.ReadSByte();
            sbyte TempDegreeF_MAX = binaryReader.ReadSByte();
            byte DecimalPosition = binaryReader.ReadByte();
            byte Day = binaryReader.ReadByte();
            byte Month = binaryReader.ReadByte();
            byte Year = binaryReader.ReadByte();
            byte Hour = binaryReader.ReadByte();
            byte Minute = binaryReader.ReadByte();
            byte Second = binaryReader.ReadByte();
            byte Spare8_0 = binaryReader.ReadByte();
            string DispUnit = binaryReader.ReadString();
            byte MeterModel = binaryReader.ReadByte();

            int[] FwVersionNumber = new int[12];
            for(int i = 0; i < 12; i++)
            {
                FwVersionNumber[i] = binaryReader.ReadByte();
            }

            int[] MeterId = new int[8];
            for(int i = 0; i < 8; i++)
            {
                MeterId[i] = binaryReader.ReadByte();
            }

            byte SignalQuality = binaryReader.ReadByte();
            byte Spare8_1 = binaryReader.ReadByte();

            int[] DayOfLogs = new int[48];
            for(int i = 0; i < 48; i++)
            {
                DayOfLogs[i] = binaryReader.ReadInt32();
            }

            short CRC = binaryReader.ReadInt16();

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
    
        public static void PlotData(int[] dataX, double[] dataY)
        {
            // var plt = new ScottPlot.Plot(400, 300);
            // plt.AddScatter(dataX, dataY);
            // plt.SaveFig("quickstart.png");
        }
    }
}