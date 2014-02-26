/** tamirtf77
 *  
 *  This is an abstract class for storing data to a block. 
 */
import java.util.ArrayList;

public abstract class Block 
{
	protected String _fileName;
	protected int _size;
	
	/** Constructs a block. */
	public Block(String fileName)
	{
		_fileName = fileName;
		_size = 0;
	}
	
	/** Sets the file's name of a block */
	public void setFileName(String fileName)
	{
		_fileName = fileName;
	}
	
	/** Clears the block. */
	public void clear()
	{
		_size = 0;
	}
	
	/** Gets the size of the block. */
	public int getSize()
	{
		return _size;
	}
	
	/** Adds numbers(keys and values) to the block. 
	 * 
	 * @param toClear if true than clears the block for next usage.
	 * @param toWrite if true than write the block when its size comes to sizeToWrite.
	 * @param sizeToWrite if the size of the block is greater or equal to sizeToWrite, than it should write the block (if toWrite is also true).
	 * @param numbers the keys and values to add to the block.
	 * @return true if the block has been written.
	 */
	abstract public boolean add(boolean toClear,boolean toWrite,int sizeToWrite, int...numbers);
	
	/** Write the block to the disk. */
	abstract public void writeBlock(boolean toClear);
	
	/** Gets the smallest key(s)-value of the block. */
	abstract public ArrayList<Integer> getSmallest();
	
	/** Adds to block(queue) key(s)-value. This method is in usage in external merge sort. */
	abstract public void addToQueue(int... numbers);
}