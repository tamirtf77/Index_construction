/** tamirtf77
 *  
 *  This class stores data about a name of the names. 
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class NameData
{
	private String _name;
	private GroupVarintEncoding _gNodesIds;
	private int _prevNode,_nameNum;
	private boolean _firstNode;
	
	public NameData(String name,int nameNum)
	{
		_name = name;
		_nameNum = nameNum;
		_gNodesIds = new GroupVarintEncoding();
		_firstNode = true;
	}
		
	/** Adds a node id which its name is _name. */
	public void addNode(int currentNode)
	{
		if (_firstNode == true)
		{
			_prevNode = _nameNum;
			_gNodesIds.clearAll();
			if ((currentNode - _prevNode) >= 0)
			{
				_gNodesIds.addToEncode(2 * (currentNode - _prevNode));
			}
		    else
		    {
		    	_gNodesIds.addToEncode(2 * (-1) * (currentNode - _prevNode) - 1); // s1 - x
			}
			_firstNode = false;
		}
		else
		{
			_gNodesIds.addToEncode(currentNode - _prevNode - 1);
		}
		_prevNode = currentNode;
	}

	/** Writes the encoded data of a name .*/
	public void write(DataOutputStream outNames,DataOutputStream outNamesString,
					  DataOutputStream outNodesListIndex,
				      DataOutputStream outNodes,byte prefixLength,
				      byte postfixLength,ArrayList<Byte> postfix,
				      int idsPointer) 
	{
		try 
		{
			outNames.writeByte(prefixLength);
			outNames.writeByte(postfixLength);
			outNodesListIndex.writeInt(idsPointer);
			for (int i = 0; i < postfix.size(); i++)
			{
				outNamesString.writeByte(postfix.get(i));
			}
			_gNodesIds.encodeGroups();
			for (int i = 0; i < _gNodesIds.size(); i++)
			{
				outNodes.writeByte(_gNodesIds.getEncodedByte(i));
			}
		} 
		catch (IOException e) {}
	}
	
	/** Gets the name. */
	public String getName()
	{
		return _name;
	}
	
	/** Gets the size of the encoded list of Ids. */
	public int getSize()
	{
		return _gNodesIds.size();
	}
	
	public int getId()
	{
		return _nameNum;
	}
	
	public void clearAll()
	{
		_gNodesIds.clearAll();
	}
}