using System;
using System.IO;

namespace SequoiaDB.Driver
{
    /// <summary>
    /// Create a variable length Byte Array.
    /// Never automatically extend the capacity.
    /// </summary>
    class ByteBuffer
    {
        // Default is Little Endian
        private bool isBigEndian = false;
        public bool IsBigEndian
        {
            get { return isBigEndian; }
            set { isBigEndian = value; }
        }

        // The capacity of buffer
        private int CAPACITY;

        // Temporary Array
        private byte[] TEMP_BYTE_ARRAY;

        // The current bytes count of buffer
        private int CURRENT_LENGTH = 0;

        // Point to the begining byte of buffer
        private int CURRENT_POSITION = 0;

        // The byte array to return
        private byte[] RETURN_ARRAY;

        /// <summary>
        /// Constructor: with Capacity specified and can not be changed
        /// <param name="cap">The capacity of ByteBuffer</param>
        /// </summary>
        public ByteBuffer(int cap)
        {
            this.CAPACITY = cap;
            this.Initialize();
        }

        /// <summary>
        /// Constructor: from an existing Byte Array
        /// </summary>
        /// <param name="bytes">The source byte array</param>
        public ByteBuffer(byte[] bytes)
        {
            this.CAPACITY = bytes.Length;
            this.Initialize();
            this.PushByteArray(bytes);
        }

        /// <summary>
        /// Return the current length of ByteBuffer
        /// </summary>
        public int Length
        {
            get
            {
                return CURRENT_LENGTH;
            }
        }

        /// <summary>
        /// Return the capacity of ByteBuffer
        /// </summary>
        public int Capacity
        {
            get
            {
                return CAPACITY;
            }
        }

        /// <summary>
        /// Return the Byte Array of this Buffer
        /// </summary>
        /// <returns>Byte[]</returns>
        public byte[] ToByteArray()
        {
            RETURN_ARRAY = new byte[CURRENT_LENGTH];
            Array.Copy(TEMP_BYTE_ARRAY, 0, RETURN_ARRAY, 0, CURRENT_LENGTH);
            return RETURN_ARRAY;
        }

        /// <summary>
        /// Initialize the ByteBuffer, apply for memory space
        /// </summary>
        public void Initialize()
        {
            TEMP_BYTE_ARRAY = new byte[CAPACITY];
            TEMP_BYTE_ARRAY.Initialize();
            CURRENT_LENGTH = 0;
            CURRENT_POSITION = 0;
        }

        /// <summary>
        /// Push a byte into ByteBuffer
        /// </summary>
        /// <param name="by">One Byte</param>
        public void PushByte(byte by)
        {
            TEMP_BYTE_ARRAY[CURRENT_LENGTH++] = by;
        }

        /// <summary>
        /// Push a byte array into ByteBuffer
        /// </summary>
        /// <param name="ByteArray">Byte Array</param>
        public void PushByteArray(byte[] ByteArray)
        {
            ByteArray.CopyTo(TEMP_BYTE_ARRAY, CURRENT_LENGTH);
            CURRENT_LENGTH += ByteArray.Length;
        }

        /// <summary>
        /// Push a short integer into ByteBuffer
        /// </summary>
        /// <param name="Num">Short Integer</param>
        public void PushShort(short Num)
        {
            byte[] tmp = BitConverter.GetBytes(Num);
            if (isBigEndian)
                Array.Reverse(tmp);
            PushByteArray(tmp);
        }

        /// <summary>
        /// Push a integer into ByteBuffer
        /// </summary>
        /// <param name="Num">Integer</param>
        public void PushInt(int Num)
        {
            byte[] tmp = BitConverter.GetBytes(Num);
            if (isBigEndian)
                Array.Reverse(tmp);
            PushByteArray(tmp);
        }

        /// <summary>
        /// Push a long integer into ByteBuffer
        /// </summary>
        /// <param name="Num">Long Integer</param>
        public void PushLong(long Num)
        {
            byte[] tmp = BitConverter.GetBytes(Num);
            if (isBigEndian)
                Array.Reverse(tmp);
            PushByteArray(tmp);
        }

        /// <summary>
        /// Pop a byte from ByteBuffer and current_position plus 1
        /// </summary>
        /// <returns>One Byte</returns>
        public byte PopByte()
        {
            byte ret = TEMP_BYTE_ARRAY[CURRENT_POSITION++];
            return ret;
        }

        /// <summary>
        /// Pop a short integer from ByteBuffer and current_position plus 2
        /// </summary>
        /// <returns>Short Integer</returns>
        public short PopShort()
        {
            // overflow
            if (CURRENT_POSITION + 1 >= CURRENT_LENGTH)
            {
                return 0;
            }
            byte[] tmp = new byte[2];
            for (int i = 0; i < 2; i++)
                tmp[i] = TEMP_BYTE_ARRAY[CURRENT_POSITION++];
            if (isBigEndian)
                Array.Reverse(tmp);
            return BitConverter.ToInt16(tmp, 0);
        }

        /// <summary>
        /// Pop a integer from ByteBuffer and current_position plus 4
        /// </summary>
        /// <returns>Integer</returns>
        public int PopInt()
        {
            // overflow
            if (CURRENT_POSITION + 3 >= CURRENT_LENGTH)
            {
                return 0;
            }
            byte[] tmp = new byte[4];
            for (int i = 0; i < 4; i++)
                tmp[i] = TEMP_BYTE_ARRAY[CURRENT_POSITION++];
            if (isBigEndian)
                Array.Reverse(tmp);
            return BitConverter.ToInt32(tmp, 0);
        }

        /// <summary>
        /// Pop a long integer from ByteBuffer and current_position plus 8
        /// </summary>
        /// <returns>Long Integer</returns>
        public long PopLong()
        {
            // overflow
            if (CURRENT_POSITION + 7 >= CURRENT_LENGTH)
            {
                return 0;
            }
            byte[] tmp = new byte[8];
            for (int i = 0; i < 8; i++)
                tmp[i] = TEMP_BYTE_ARRAY[CURRENT_POSITION++];
            if (isBigEndian)
                Array.Reverse(tmp);
            return BitConverter.ToInt64(tmp, 0);
        }

        /// <summary>
        /// Pop a byte array with length Length from ByteBuffer and current_position plus Length
        /// </summary>
        /// <param name="Length">The length of byte array</param>
        /// <returns>Byte Array</returns>
        public byte[] PopByteArray(int Length)
        {
            // overflow
            if (CURRENT_POSITION + Length > CURRENT_LENGTH)
            {
                return new byte[0];
            }
            byte[] ret = new byte[Length];
            Array.Copy(TEMP_BYTE_ARRAY, CURRENT_POSITION, ret, 0, Length);
            CURRENT_POSITION += Length;
            return ret;
        }
    }
}