using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;

namespace SerializationTest
{
    class Program
    {
        static void SerializeTest(object o, string expected)
        {
            var bytes = BinarySerialize(o);
            var actual = BitConverter.ToString(bytes);
            if (expected != actual)
                System.Console.WriteLine("failed");
        }
        static void DeserializeTest(string arr, object expected)
        {
            var bytes = StringToByteArray(arr);
            var actual = BinaryDeserialize(bytes);
            if (!actual.Equals(expected))
                System.Console.WriteLine("failed");
        }

        static void Main(string[] args)
        {
            var _0 = (object)0L;
            SerializeTest(_0, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B");
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            var _16 = (object)16L;
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-10-00-00-00-00-00-00-00-0B", _16);
            SerializeTest(_16, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-10-00-00-00-00-00-00-00-0B");
            SerializeTest(_0, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B");
            SerializeTest(_16, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-10-00-00-00-00-00-00-00-0B");
            SerializeTest(_0, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B");
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            var _2 = (object)2L;
            SerializeTest(_2, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-02-00-00-00-00-00-00-00-0B");
            SerializeTest(_16, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-10-00-00-00-00-00-00-00-0B");
            SerializeTest(_0, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B");
            SerializeTest(_16, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-10-00-00-00-00-00-00-00-0B");
            SerializeTest(_0, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B");
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            DeserializeTest("00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-00-00-00-00-00-00-00-00-0B", _0);
            var _4 = (object)4L;
            SerializeTest(_4, "00-01-00-00-00-FF-FF-FF-FF-01-00-00-00-00-00-00-00-04-01-00-00-00-0C-53-79-73-74-65-6D-2E-49-6E-74-36-34-01-00-00-00-07-6D-5F-76-61-6C-75-65-00-09-04-00-00-00-00-00-00-00-0B");
        }

        // taken from SO
        public static byte[] StringToByteArray(string hex)
        {
            // Get the separator character.
            char separator = '-';

            // Split at the separators.
            string[] pairs = hex.Split(separator);
            byte[] bytes = new byte[pairs.Length];
            for (int i = 0; i < pairs.Length; i++)
                bytes[i] = Convert.ToByte(pairs[i], 16);
            return bytes;
        }

        public static byte[] BinarySerialize(object o)
        {
            var serializationStream = new System.IO.MemoryStream();
            var BF = new BinaryFormatter();

            BF.Serialize(serializationStream, o);

            var ret = serializationStream.ToArray();

            serializationStream.Dispose();

            return ret;
        }

        public static object BinaryDeserialize(byte[] anchor)
        {
            var serializationStream = new System.IO.MemoryStream(anchor);
            var BF = new BinaryFormatter();
            var ret = BF.Deserialize(serializationStream);

            serializationStream.Dispose();

            return ret;
        }
    }
}
