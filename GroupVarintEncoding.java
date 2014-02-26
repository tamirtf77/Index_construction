/** tamirtf77
 *  
 *  This class implements group varint encoding, as we saw in class.
 */
import java.util.ArrayList;

public class GroupVarintEncoding
{
	
	private static final int BYTE_SIZE = 8;
	private static final int PREFIX_SIZE = 2;
	private static final char PADDING = '0';
	private static final String EMPTY = "";
	private static final int FROM_LEFT = 1;
	private static final int FROM_RIGHT = 0;
	private static final int NUMBERS_IN_GROUP = 4;
	private static final int BINARY_RADIX = 2;
	
	private ArrayList<Integer> _diffsToEncode;
	private ArrayList<Byte> _encodeToBytes;
	private ArrayList<Byte> _decodeToBytes;
	private ArrayList<Integer> _diffsDecode;
	
	public GroupVarintEncoding()
	{
		_diffsToEncode = new ArrayList<Integer>();
		_encodeToBytes = new ArrayList<Byte>();
		_decodeToBytes = new ArrayList<Byte>();
		_diffsDecode = new ArrayList<Integer>();
	}
	
	/** Adds an integer number to the list of the numbers to encode.
	 *  
	 * @param diff - an integer number, actually a difference between to numbers in a positing list.
	 */
	public void addToEncode(int diff)
	{
		_diffsToEncode.add(diff);
	}
	
	/** Adds a byte (which is read from a file) to a list of bytes in order to decode a positing list.
	 * 
	 * @param b - a byte.
	 */
	public void addDecoded(byte b)
	{
		_decodeToBytes.add(b);
	}
	
	/** Clears all the lists. */
	public void clearAll()
	{
		_diffsToEncode.clear();
		_encodeToBytes.clear();
		_decodeToBytes.clear();
		_diffsDecode.clear();
	}
	
	/** Encodes the numbers to groups, four in a group. */
	public void encodeGroups()
	{
		String prefix = EMPTY, current = EMPTY;
		int diffsRead = 0, bytesAdded = 0, iLengthInfo = 0;
		byte b = 0;
		_encodeToBytes.add(b);
		for (int i=0; i < _diffsToEncode.size(); i++)
		{
			current = padding(Integer.toBinaryString(_diffsToEncode.get(i)),BYTE_SIZE,FROM_LEFT);
			bytesAdded += divideToBytes(current,_encodeToBytes);
			prefix +=  padding(Integer.toBinaryString( (current.length() / BYTE_SIZE) - 1),PREFIX_SIZE,FROM_LEFT);
			diffsRead++;
			if (diffsRead == NUMBERS_IN_GROUP)
			{
				_encodeToBytes.set(iLengthInfo, (byte)Integer.parseInt(prefix,BINARY_RADIX));
				_encodeToBytes.add(b);
				prefix = EMPTY;
				diffsRead = 0;
				iLengthInfo += bytesAdded+1;
				bytesAdded = 0;
			}
		}
		if (diffsRead == 0)
		{
			_encodeToBytes.remove(_encodeToBytes.size()-1);
		}
		else // (diffsRead > 0)
		{
			prefix = padding(prefix,BYTE_SIZE,FROM_RIGHT);
			_encodeToBytes.set(iLengthInfo, (byte)Integer.parseInt(prefix,BINARY_RADIX));
		}
	}
	
	/** Divides a string to bytes.
	 * 
	 * @param s a string which represents a number.
	 * @param encodeToBytes the list to store the bytes of the string.
	 * @return the number of bytes the string captured.
	 */
	private int divideToBytes(String s, ArrayList<Byte> encodeToBytes) 
	{
		int numOfBytes = s.length() / BYTE_SIZE;
		byte b;
		for (int i=0; i < numOfBytes; i++)
		{
			b = (byte)Integer.parseInt(s.substring(i*BYTE_SIZE, i*BYTE_SIZE + BYTE_SIZE), BINARY_RADIX);
			encodeToBytes.add(b);
		}
		return numOfBytes;
	}
	
	/** Returns the number of bytes of the encoded list. */
	public int size()
	{
		return _encodeToBytes.size();
	}
	
	/** Returns the number of bytes of the decoded list. */
	public int decodedSize()
	{
		return _diffsDecode.size();
	}
	/** Returns a byte from the encoded bytes list according to an index i. */
	public byte getEncodedByte(int i)
	{
		return _encodeToBytes.get(i);
	}
	
	/** Returns an integer from the decoded list(actually the decode differences posting list)
	 *  according to an index i. */
	public int getDecoded(int i)
	{
		return _diffsDecode.get(i);
	}
	
	/** Decodes a list, according to its groups (4 numbers in a group) */
	public void decodeGroups()
	{
		int i = 0,currentBytesOfGroup = 0,totalBytesleftToDecode = _decodeToBytes.size(),numbersInTheGroup = BYTE_SIZE/PREFIX_SIZE;
		String diff = EMPTY;
		int[] groupSizes = new int[BYTE_SIZE/PREFIX_SIZE];
		while (i < _decodeToBytes.size())
		{
			currentBytesOfGroup = getGroupSizes(_decodeToBytes.get(i++),groupSizes,FROM_LEFT);
			totalBytesleftToDecode--; // It reads a byte of a prefix.
			if (totalBytesleftToDecode < currentBytesOfGroup) // in the encoding it padded from right with zeros.
			{
				numbersInTheGroup-= (currentBytesOfGroup-totalBytesleftToDecode);
			}
			for (int j = 0; j < numbersInTheGroup; j++)
			{
				for (int k = 0; k < groupSizes[j]; k++) // get each difference in a group.
				{
					diff = diff + getBinary(_decodeToBytes.get(i++),FROM_LEFT);
				}
				_diffsDecode.add(Integer.parseInt(diff,BINARY_RADIX));
				diff = EMPTY;
			}
			totalBytesleftToDecode -= currentBytesOfGroup;
		}
	}
	
	/** Gets a group sizes of bytes, according to the first byte in a group.
	 * 
	 * @param sizesByte the first byte in a group. This byte has the sizes in bytes of the next 4 numbers in a group.
	 * @param groupSizes saves the numbers of bytes for each number in a group.
	 * @param direction the direction to padding the binary representation of a number - from left or right.
	 * @return the total number of bytes of the 4 numbers are captured.
	 */
	private int getGroupSizes(Byte sizesByte, int[] groupSizes,int direction)
	{
		int bytesOfGroup = 0;
		String binary = getBinary(sizesByte,direction);
		for (int i = 0, j = 0; i < BYTE_SIZE; i += PREFIX_SIZE, j++)
		{
			groupSizes[j] = Integer.parseInt(binary.substring(i, i+PREFIX_SIZE),BINARY_RADIX) + 1; // +1 because 00 means the number
			// captured 1 bytes, 01 means the number captured 2 bytes and so on.
			bytesOfGroup += groupSizes[j];
		}
		return bytesOfGroup;
	}
	
	/** Gets the a string of binary representation of a number in a byte.
	 *  
	 * @param b   a number in a byte.
	 * @param direction the direction to padding the binary representation of a number - from left or right.
	 * @return
	 */
	private String getBinary(Byte b, int direction)
	{
		String binary = Integer.toBinaryString(b);
		if (b < 0) // in case it begin with 1 so Java looks it is a negative number.
		{
			binary = binary.substring(binary.length()-BYTE_SIZE, binary.length());
		}
		binary = padding(binary,BYTE_SIZE,direction);
		return binary;
	}

	/** padding a binary number(String) to a multiple of 8.
	 * 
	 * @param binaryNum a string which represents a binary number.
	 * @param size the number of bits required. it's supposed to be 8,16,24 or 32.
	 * @param direction the direction to padding the binary representation of a number - from left or right.
	 * @return
	 */
	private String padding(String binaryNum,int size,int direction)
	{
		int len = binaryNum.length();
		int remainder = len % size;
		if (remainder != 0) // if the length is not a complete multiple of 8 bits in a byte.
		{
			for (int i=1; i <=(size - remainder); i++)
			{
				if (direction == FROM_LEFT)
				{
					binaryNum = PADDING + binaryNum;
				}
				else
				{
					binaryNum = binaryNum + PADDING;
				}
			}
		}
		return binaryNum;
	}
}