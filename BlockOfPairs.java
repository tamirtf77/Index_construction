/** tamirtf77
 *  
 *  This class is for storing block of pair i.e. block of key and value. 
 */

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class BlockOfPairs extends Block
{

	private static final byte PAIR_SIZE = 8;
	private Map<Integer,TreeSet<Integer>>_pairs;
	private TreeMap<Integer,TreeMap<Integer,Integer>> _pQueue;

	/** Constructs block of pairs. */
	public BlockOfPairs(String fileName)
	{
		super(fileName);
		_pairs = new TreeMap<Integer, TreeSet<Integer>>();
		_pQueue = new TreeMap<Integer, TreeMap<Integer,Integer>>();
	}
	
	/** Clears the block. */
	public void clear()
	{
		super.clear();
		_pairs.clear();
	}
	
	/** Adds key and value. if toWrite is true, then write if the size of all 
	 *  keys and values in bytes is the size of a block. 
	 *  returns true if the buffer is full, otherwise returns false.
	 */
	@Override
	public boolean add(boolean toClear,boolean toWrite,int sizeToWrite, int... numbers) 
	{
		// key == numbers[0], value == numbers[1];
		if (_pairs.containsKey(numbers[0]))
		{
			_pairs.get(numbers[0]).add(numbers[1]);
		}
		else
		{
			TreeSet<Integer> values = new TreeSet<Integer>();
			_pairs.put(numbers[0],values);
			_pairs.get(numbers[0]).add(numbers[1]);
		}
		_size += PAIR_SIZE;
		if ((toWrite == true) && (_size >= sizeToWrite))
		{
			writeBlock(toClear);
			return true;
		}
		return false;
	}
	
	public void writeBlock(boolean toClear)
	{
		try 
		{
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_fileName,true)));
			Integer key;
			TreeSet<Integer> it;
			for (Map.Entry<Integer, TreeSet<Integer>> entry : _pairs.entrySet()) 
			{
				key = entry.getKey();
				it = entry.getValue();
				for (Integer value : it) 
				{
					out.writeInt(key);
					out.writeInt(value);
				}
			}
			out.flush();
			out.close();
		}
		catch (IOException e) {}
		if (toClear)
		{
			clear();
		}
		System.gc();
	}

	public ArrayList<Integer> getSmallest()
	{
		ArrayList<Integer> nums = new ArrayList<Integer>(3);
		if (_pQueue.size() > 0)
		{
			nums.add(_pQueue.firstKey());
			nums.add(_pQueue.firstEntry().getValue().firstKey());
			nums.add(_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()));
			_pQueue.firstEntry().getValue().remove(_pQueue.firstEntry().getValue().firstKey());
			if (_pQueue.get(_pQueue.firstKey()).size() == 0)
			{
				_pQueue.remove(_pQueue.firstKey());
			}
			return nums;
		}
		return null;
	}
	
	public void addToQueue(int... numbers) 
	{
		// key = numbers[0], value = numbers[1], keyFile = numbers[2];
		if (_pQueue.containsKey(numbers[0]))
		{
			_pQueue.get(numbers[0]).put(numbers[1], numbers[2]);
		}
		else
		{
			TreeMap<Integer,Integer> valueAndFileNum = new TreeMap<Integer,Integer>();
			valueAndFileNum.put(numbers[1], numbers[2]);
			_pQueue.put(numbers[0], valueAndFileNum);
		}
	}
}