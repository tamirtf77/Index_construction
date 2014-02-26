/** tamirtf77
 *  
 *  This class is for storing block of three i.e. block of key1,key2 and value. 
 */

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BlockOfThree extends Block
{
	private static final byte THREE_SIZE = 12;
	private Map<Integer, TreeMap<Integer, List<Integer>>>_threes;
	private TreeMap<Integer,TreeMap<Integer,List<Integer>>> _pQueue;
	
	public BlockOfThree(String fileName)
	{
		super(fileName);
		_threes = new TreeMap<Integer, TreeMap<Integer,List<Integer>>>();
		_pQueue = new TreeMap<Integer, TreeMap<Integer,List<Integer>>>();
	}
	
	public void clear()
	{
		super.clear();
		_threes.clear();
	}
	
	/** Adds key and value. if toWrite is true, then write if the size of all 
	 *  keys and values in bytes is the size of a block. 
	 *  returns true if the buffer is full, otherwise returns false.
	 */
	@Override
	public boolean add(boolean toClear, boolean toWrite,int sizeToWrite, int...numbers) 
	{
		//key1 == numbers[0], key2 == numbers[1], value == numbers[2];
		if (_threes.containsKey(numbers[0]))
		{
			if(!_threes.get(numbers[0]).containsKey(numbers[1]))
			{
				_threes.get(numbers[0]).put(numbers[1], new ArrayList<Integer>());
			}
		}
		else
		{
			TreeMap<Integer,List<Integer>> insideMap = new TreeMap<Integer,List<Integer>>();
			insideMap.put(numbers[1], new ArrayList<Integer>());
			_threes.put(numbers[0], insideMap);
		}
		_threes.get(numbers[0]).get(numbers[1]).add(numbers[2]);
		_size += THREE_SIZE;
		if ((toWrite == true) && (_size > sizeToWrite))
		{
			writeBlock(toClear);
			return true;
		}
		return false;
	}

	@Override
	public void writeBlock(boolean toClear)
	{
		try 
		{
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_fileName,true)));
			int key1,key2 = 0;
			TreeMap<Integer, List<Integer>> it1;
			List<Integer> it2 = null;
			for (Map.Entry<Integer, TreeMap<Integer, List<Integer>>> entry1 : _threes.entrySet()) 
			{
				key1 = entry1.getKey();
				it1 = entry1.getValue();
				for (Map.Entry<Integer, List<Integer>> entry2 : it1.entrySet()) 
				{
					key2 = entry2.getKey();
					it2 = entry2.getValue();
					for (Integer value : it2) 
					{
						out.writeInt(key1);
						out.writeInt(key2);
						out.writeInt(value);
					}
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
		ArrayList<Integer> nums = new ArrayList<Integer>(4);
		if (_pQueue.size() > 0)
		{
			nums.add(_pQueue.firstKey());
			nums.add(_pQueue.firstEntry().getValue().firstKey());
			nums.add(_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()).get(0));
			nums.add(_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()).get(1));
			_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()).remove(1);
			_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()).remove(0);
			if (_pQueue.firstEntry().getValue().get(_pQueue.firstEntry().getValue().firstKey()).size() == 0)
			{
				_pQueue.firstEntry().getValue().remove(_pQueue.firstEntry().getValue().firstKey());
			}
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
		if (_pQueue.containsKey(numbers[0]))
		{
			if(_pQueue.get(numbers[0]).containsKey(numbers[1]))
			{
				_pQueue.get(numbers[0]).get(numbers[1]).add(numbers[2]);
				_pQueue.get(numbers[0]).get(numbers[1]).add(numbers[3]);
			}
			else
			{
				ArrayList<Integer> temp = new ArrayList<Integer>();
				temp.add(numbers[2]);
				temp.add(numbers[3]);
				_pQueue.get(numbers[0]).put(numbers[1],temp);
			}
		}
		else
		{
			ArrayList<Integer> temp = new ArrayList<Integer>();
			temp.add(numbers[2]);
			temp.add(numbers[3]);
			TreeMap<Integer,List<Integer>> insideMap = new TreeMap<Integer,List<Integer>>();
			insideMap.put(numbers[1],temp);
			_pQueue.put(numbers[0], insideMap);
		}
	}
}