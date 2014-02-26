/** tamirtf77
 *  
 *  This class is performs external merge sort. 
 */

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;


public class ExtMergeSort 
{
	private static final String EMPTY = "";
	private static final byte PAIR = 2;
	private static final byte THREE = 3;
	private Block _block;
	private byte _type;
	
	/** Constructs the block to use according to the type PAIR/THREE. */
	public ExtMergeSort(byte type)
	{
		_type = type;
		if (type == PAIR)
		{
			_block = new BlockOfPairs(EMPTY); 
		}
		if (type == THREE)
		{
			_block = new BlockOfThree(EMPTY);
		}
	}
	
	/** Merges the runs (as we called it in class) which right now are on the disk */
	public void mergeSortRuns(long numOfRuns, String firstPartOfName,String fileSuffix,int blockSize) 
	{
		long low = 1;
    	Runtime runtime = Runtime.getRuntime();
    	long freeBlocks = (runtime.freeMemory()/IndexWriter.BUFFER_BLOCK);
    	
    	long usedBlocks = numOfRuns;
    	long numOfNewRuns = 0;
    	//if (numOfRuns > freeBlocks)
    	//{
    	//	usedBlocks = freeBlocks;
    	//}
		if (freeBlocks > 3)
		{
			freeBlocks = 3;
	    }
        if (freeBlocks >= usedBlocks) 
		{
			while (numOfRuns != 1)
			{
				merge(low,firstPartOfName,fileSuffix,usedBlocks,_block);
				low += usedBlocks;
				if (low > numOfRuns)
				{
					numOfRuns = (numOfRuns / usedBlocks) + (numOfRuns % usedBlocks);
					low = 1;
				}
			}
		}
        else
        {
			if (freeBlocks > 2)
			{
				freeBlocks = 2;
			}
			while (numOfRuns > 0)
			{
				merge(low,firstPartOfName,fileSuffix,freeBlocks,_block);
				low += freeBlocks;
				numOfRuns -= freeBlocks;
				numOfNewRuns++;
			}
			low = 1;
			while (numOfNewRuns != 1)
			{
				for (; low <= numOfNewRuns; low += numOfNewRuns)
				{
					merge2(low,firstPartOfName,fileSuffix,numOfNewRuns,_block);
				}
				numOfNewRuns = (numOfNewRuns / numOfNewRuns) + (numOfNewRuns % numOfNewRuns);
				low = 1;
			}
        }
	}
	
	/** Merges the runs on the disk */
	private void merge2(long fileNumber, String firstPartOfName,String fileSuffix,long n,Block block)
	{
		File file1,file2;
		long temp = fileNumber , temp2 = fileNumber;
		ArrayList<DataInputStream> inputs = new ArrayList<DataInputStream>();
    	ArrayList<File> files = new ArrayList<File>();
    	File f = null;
		block.setFileName(firstPartOfName + temp + "_temp" + fileSuffix);
    	for (int i = 0; i < n; i++)
    	{
    		f = new File(firstPartOfName + fileNumber + fileSuffix);
    		if (f.exists())
    		{
        		files.add(f);
        		inputs.add(openDataInputStream(firstPartOfName + temp + fileSuffix));
        		fileNumber++; temp++;
    		}
    	}
		if (_type == PAIR)
		{
			mergePairs(inputs, block);
		}		
		if (_type == THREE)
		{
			mergeThrees(inputs, block);
		}
		for (int i = 0; i < n; i++)
		{
			closeDataInputStream(inputs.get(i));
			files.get(i).delete();
		}
			
		file1 = new File(firstPartOfName + temp2 + "_temp" + fileSuffix);
		file2 = new File(firstPartOfName + ( (temp2 / n) + (temp2 % n) ) + fileSuffix);
		file1.renameTo(file2);	 
	}
	
	/** Merges runs of threes. */
	private void mergeThrees(ArrayList<DataInputStream> inputs, Block block) 
	{
		int inputsAvailable = inputs.size();
		try
		{
			for (int i = 0; i < inputs.size(); i++)
			{
				block.addToQueue(inputs.get(i).readInt(),inputs.get(i).readInt(),
							     inputs.get(i).readInt(),i);
			}
			while (inputsAvailable > 0)
			{
				readToQueue(inputs,block);
				inputsAvailable--;
			}
		}
		catch (EOFException e){}
		catch (IOException e){}
		block.writeBlock(true);
	}

	/** Reads to queue(BlockOfThree) each three from each run in order to get the smallest,
	 *  so it could know from which run to read the next three. 
	 */
	private void readToQueue(ArrayList<DataInputStream> inputs, Block block) 
	{
		ArrayList<Integer> values = null;
		try
		{
			while (true)
			{
				values = block.getSmallest();
				if (values != null)
				{
					block.add(true,true,IndexWriter.BUFFER_BLOCK,values.get(0),values.get(1),values.get(2));
					block.addToQueue(inputs.get(values.get(3)).readInt(),inputs.get(values.get(3)).readInt(),
						 	 inputs.get(values.get(3)).readInt(),values.get(3));
				}
				else
				{
					break;
				}
			}
		}
		catch (EOFException e){}
		catch (IOException e){}
	}

	/** Merges runs of pairs. */
	private void mergePairs(ArrayList<DataInputStream> inputs, Block block) 
	{
		int inputsAvailable = inputs.size();
		try
		{
			for (int i = 0; i < inputs.size(); i++)
			{
				block.addToQueue(inputs.get(i).readInt(),inputs.get(i).readInt(),i);
			}
			while (inputsAvailable > 0)
			{
				readToQueue2(inputs,block);
				inputsAvailable--;
			}
		}
		catch (EOFException e){}
		catch (IOException e){}
		block.writeBlock(true);
	}

	/** Reads to queue(BlockOfPairs) each pair from each run in order to get the smallest,
	 *  so it could know from which run to read the next pair. 
	 */
	private void readToQueue2(ArrayList<DataInputStream> inputs, Block block) 
	{
		ArrayList<Integer> values = null;
		try
		{
			while (true)
			{
				values = block.getSmallest();
				if (values != null)
				{
					block.add(true,true,IndexWriter.BUFFER_BLOCK,values.get(0),values.get(1));
					block.addToQueue(inputs.get(values.get(2)).readInt(),inputs.get(values.get(2)).readInt(),
		                     values.get(2));
				}
				else
				{
					break;
				}
			}
		}
		catch (EOFException e){}
		catch (IOException e){}
	}
	
	/** Merges the whole files (not using a queue). */
	private void merge(long fileNumber, String firstPartOfName,String fileSuffix,long n,Block block)
	{
		File file;
		int key = 0, value = 0 , key2 = 0;
		DataInputStream in;
		for (long i = 0; i < n; i++)
		{
			file = new File(firstPartOfName + fileNumber + fileSuffix);
			if (file.exists())
			{
				in = openDataInputStream(firstPartOfName + fileNumber + fileSuffix);
				try 
				{
					while(true)
					{
						if (_type == PAIR)
						{
							key = in.readInt();
							value = in.readInt();
							block.add(true,false,IndexWriter.BUFFER_BLOCK,key,value);
						}
						
						if(_type == THREE)
						{
							key = in.readInt();
							key2 = in.readInt();
							value = in.readInt();
							block.add(true,false,IndexWriter.BUFFER_BLOCK,key,key2,value);
						}
					}
				}
				catch (EOFException e){}
				catch (IOException e){}
				closeDataInputStream(in);
				file.delete();
			}
			fileNumber++;
		}
		block.setFileName(firstPartOfName + (fileNumber/n) + fileSuffix);
		block.writeBlock(true);
	}
	
	/** Opens a DataOutputStream to the file which its name is given by fileName. */
	private DataInputStream openDataInputStream(String fileName)
	{
		DataInputStream in = null;
		try 
		{
			in = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
		} 
		catch (FileNotFoundException e) {}
		return in;
	}
	
	/** Closes a DataOutputStream. */
	private void closeDataInputStream(DataInputStream in)
	{
    	if (in != null) 
        {
    		try 
            {
				in.close();
			} 
            catch (IOException e) {}
        }
	}
}