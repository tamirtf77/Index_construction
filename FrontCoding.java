/** tamirtf77
 *  
 *  This class implements front coding, as we saw in class.
 */

import java.util.ArrayList;

public class FrontCoding 
{
	
	private static final String PADDING = "0";
	private static final int BYTE_SIZE = 8;
	private static final int BINARY_RADIX = 2;
	
	
	private ArrayList<Byte> _prevBytes,_currentBytes,_currentPosfixBytes;
	private int _currentPrefixLength, _currentPostfixLength;
	
	public FrontCoding()
	{
		_prevBytes = new ArrayList<Byte>();
		_currentBytes = new ArrayList<Byte>();
		_currentPosfixBytes = new ArrayList<Byte>();
		_currentPrefixLength = 0;
		_currentPostfixLength = 0;
	}
	
	/** Clears all the array lists. */
	public void clearAll()
	{
		_prevBytes.clear();
		_currentBytes.clear();
		_currentPosfixBytes.clear();
	}
	
	/** Encodes a word.*/
	public void encodeWord(String word)
	{
		copy(_prevBytes,_currentBytes);
		_currentBytes.clear();
		_currentPosfixBytes.clear();
		divideToBytes(word,_currentBytes);
		setPrefixPostfixLengths();
	}
	
	/** Copy bytes. */
	private void copy(ArrayList<Byte> prevBytes, ArrayList<Byte> currentBytes) 
	{	
		_prevBytes.clear();
		for (int i = 0; i < currentBytes.size(); i++)
		{
			prevBytes.add(currentBytes.get(i));
		}
	}

	/** Divides a string to bytes.
	 * 
	 * @param s a string which represents a number.
	 * @param encodeToBytes the list to store the bytes of the string.
	 *@return the number of bytes the string captured.////
	 */
	private void divideToBytes(String s, ArrayList<Byte> encodeToBytes) 
	{
		char[] chars = s.toCharArray();
		for (int i = 0; i < chars.length; i++)
		{
			encodeToBytes.add( (byte)chars[i] );
		}
	}
	
	/** Sets the prefix and postfix lengths of a word according to the previous word.*/
	private void setPrefixPostfixLengths()
	{
		if (_prevBytes.size() == 0)
		{
			_currentPrefixLength = 0;
			_currentPostfixLength = _currentBytes.size();
			return;
		}
		int i = 0;
		while ( (i < _prevBytes.size() ) && 
				(i < _currentBytes.size()) && 
				(_prevBytes.get(i) == _currentBytes.get(i)) )
		{
			i++;
		}
		_currentPrefixLength = i;
		_currentPostfixLength = _currentBytes.size() - _currentPrefixLength;
	}
	
	/** Gets the prefix length. */
	public int getPrefixLength()
	{
		return _currentPrefixLength;
	}
	
	/** Gets the postfix length. */
	public int getPostfixLength()
	{
		return _currentPostfixLength;
	}
	
	/** Gets the postfix bytes of a word */
	public ArrayList<Byte> getPostfixBytes()
	{
		String temp;
		for (int i = _currentPrefixLength; i < _currentBytes.size(); i++)
		{
			temp = padding(Integer.toBinaryString(_currentBytes.get(i)),BYTE_SIZE);
			_currentPosfixBytes.add((byte) Integer.parseInt(temp,BINARY_RADIX));
		}
		return _currentPosfixBytes;
	}
	
	/** Paddings from left the binaryNum with zeros to a multiple of 8 bits. */
	private String padding(String binaryNum, int size)
	{
		int len = binaryNum.length();
		int remainder = len % size;
		if (remainder != 0) // if the length is not a complete multiple of 8 bits in a byte.
		{
			for (int i=1; i <=(size - remainder); i++)
			{
					binaryNum = PADDING + binaryNum;
			}
		}
		return binaryNum;
	}
}