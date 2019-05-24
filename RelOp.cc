#include <iostream>
#include <fstream>
#include "RelOp.h"
#include "EfficientMap.h"
#include <bitset>
#include<stdio.h>
#include<string.h>

using namespace std;

ostream& operator<<(ostream& _os, RelationalOp& _op) {

	return _op.print(_os);
}

Scan::Scan(Schema& _schema, DBFile& _file, string _tblName) {

  schema = _schema;
  file = _file;
  tblName = _tblName + ".dat";
  char* z = const_cast<char*>(tblName.c_str());
  file.Open(z);
}


Scan::~Scan() {

	file.Close();

}

bool Scan::GetNext(Record& _record) 
{
	//cout << "SCAN" << endl;
	char* z = const_cast<char*>(tblName.c_str());
	bool isOpen = false;
	int ret = file.GetNext(_record);
	//_record.print(cout, schema);
	if (true == ret)
	{
		//_record.print(cout, schema);
		/*if(false == isOpen)
		{
			if(file.Open(z))
			{
			isOpen = true;
			}
		}*/
		return true;
	}
	else{
		return false;
	}
}


ostream& Scan::print(ostream& _os) {
	_os << "SCAN " << tblName << "\n {" << schema << "}" << endl;
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
  schema = _schema;
  predicate = _predicate;
  constants = _constants;
  producer = _producer;

}



Select::~Select() {

}

bool Select::GetNext(Record& _record)
{
	//cout<<"SELECT"<<endl;
	while(1)
	{
		bool ret = producer->GetNext(_record);
		if(false == ret)
		{
			return false;
		}
		else
		{
			ret = predicate.Run(_record,constants);
			if(ret == true)
			{
			//_record.print(cout,schema);//commented in
				return true;
			}
		}
	}
}

ostream& Select::print(ostream& _os) {
	/*while (constanst) {
		constants.print(cout, schema);
		cout << endl;
	//}*/
	/*for(int i = 0; i < 4; i++)
	{
		string c = constants.GetColumn(i);
		cout << "Literal: " << endl;
	}
	if(predicate.numAnds != 0){

*/	_os << "SELECT {" << schema << "}\n{" << predicate << "}" <<*producer <<endl;	//}

	return _os;
}

Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

  schemaIn = _schemaIn;
  schemaOut = _schemaOut;
  numAttsInput = _numAttsInput;
  numAttsOutput = _numAttsOutput;
  keepMe = _keepMe;
  producer = _producer;

}

Project::~Project() {

}

bool Project::GetNext(Record& _record)
{
	//cout <<"PROJECT"<< endl;
	bool ret = producer->GetNext(_record);
	if(true == ret)
	{
		//cout<<"Schema IN:"<<endl;
		//_record.print(cout, schemaIn);//added
		//cout<<"Schema Out"<<endl;
		//_record.print(cout, schemaOut);//added
		_record.Project(keepMe,numAttsOutput,numAttsInput);
		return true;
	}
	else
	{
		return false;
	}
}


ostream& Project::print(ostream& _os) {
	_os << "PROJECT {" << schemaIn << "}\n SCHEMAOUT{" << schemaOut << "}\n NUM ATTS IN:" << numAttsInput << "}\nNUM ATTS OUT: {" << numAttsOutput<<*producer<< endl; 	
	return _os;
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;
}

Join::~Join() {

}

bool Join::GetNext(Record& _record)
{
	//cout<<"JOIN"<<endl;
	int size = predicate.numAnds;
	int l = schemaLeft.GetNumAtts();
	int r = schemaRight.GetNumAtts();
	while(1)
	{	
		if(shj == true)
		{
			while(ltimes == true)
			{
				if(left->GetNext(_record) == true)
				{
					recL = _record;
					rightMap.MoveToStart();
					data++;
					if (data%1500 == 0)
					{
						rtimes = true; 
						ltimes = false;
						rtimes = true; 
							
					}
					leftMap.Insert(recL,data);
					while(rightMap.AtEnd() != true)//___HAS TO BE WHILE___
					{ 
						recordR = rightMap.CurrentKey();
						rightMap.Advance();
						bool x3 = predicate.Run(leftMap.CurrentKey(),recordR);
						if(x3 == true)
						{
							//cout<<"SECOND MERGE"<<endl;
							//counter++;
							fin.AppendRecords(leftMap.CurrentKey(),recordR,l,r);
							//cout<<"HERE 2"<<endl;
							//cout<<"COUNT:"<< counter<<endl;
							//cout<< "MERGE"<<endl;
							//fin.print(cout,schemaOut);
							//cout<<endl;
							_record = fin;//added
							//_record.print(cout,schemaOut);
							return true;
						}
					}
					
				}
				else{
					done = true;
					rtimes = true;
					ltimes = false;
					if(done2 == true)
					{
						return false;
					}
				}
			}
			while(rtimes == true)
			{
				if(right->GetNext(_record) == true)
				{
					recR = _record;
					data2++;
					leftMap.MoveToStart();
					rightMap.Insert(recR,data);
					if (data2%1500 == 0)
					{
						ltimes= true; 
						rtimes = false;
					}
					while(leftMap.AtEnd() != true)
					{
						recordL = leftMap.CurrentKey();
						leftMap.Advance();
						bool x2 = predicate.Run(recordL,rightMap.CurrentKey());
						if(x2 == true)
						{
							fin.AppendRecords(recordL,rightMap.CurrentKey(),l,r);
							_record = fin;
							return true;
						}		
					}
				}
				else
				{
					done2 = true;
					ltimes = true; 
					rtimes = false;
					if(done == true)
					{
						return false;
					}
				}	
			}
		}
	}			
}


ostream& Join::print(ostream& _os) {

	_os << "JOIN {SCHEMALEFT:" << schemaLeft << "}\n{SCHEMARIGHT:" << schemaRight << "}\n{SCHEMAOUT:" << schemaOut << "}" << *left << *right<<endl; 	
	return _os;
}
DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
  schema = _schema;
  producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record& _record)
{
	//cout<<"DISTINCT"<<endl;
	OrderMaker order(schema);
	while(producer->GetNext(_record))
	{
		// cout << "DISTINCT RECORD: ";
		//_record.print(cout,schema);
		int count = 0;
		if(first == true)//get the first record
		{
			//if(rlist.AtStart() == true)
			//{
				Record re;
				re = _record;
				rlist.Insert(re);//insert first Record
				r = _record;
			//}
			first = false;
			return true;
		}
		int ret1 = order.Run(r,_record);//check if record in list is the same as record
		if(ret1 != 0)//if not
		{
			start = 1;
			rlist.MoveToStart();
			while(!rlist.AtEnd())
			{
				Record rd;
				rd = rlist.Current();
				bool ret = order.Run(rd,_record);
				if(order.Run(rd,_record) == 0)
				{
					count++;
				}
				rlist.Advance();
			}
			if(count == 0 && start != 0)
			{
				Record rd;
				rd = _record;
				rlist.Insert(rd);
				return true;
			}
		}
	}
	if(producer->GetNext(_record) != true)
	{
		return false;
	}
}

ostream& DuplicateRemoval::print(ostream& _os) {
	_os << "DISTINCT {" << schema << "}\n{" << *producer << "}\n";	
	return _os;

}

Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
  
  schemaIn = _schemaIn;
  schemaOut = _schemaOut;
  compute = _compute;
  producer = _producer;



}
Sum::~Sum() {


}
bool Sum::GetNext(Record& _record)
{
	//cout<<"SUM"<<endl;
	if(sum == false){
		//int t = 0;
		
		while(producer->GetNext(_record))
		{
			//_record.print(cout,schemaIn);
			//_record.print(cout,schemaOut);
			runningSum = compute.Apply(_record,results,r);	
			if (runningSum == Type::Float){
				total +=r;
				//t = to_string(total);

			}			
		

			//return true;
			
			
		}
		if (runningSum == Type::Float){
		t = to_string(total);
			//cout<<t<<endl;
		}
		if(producer->GetNext(_record) != true && total != 0)// last record
		{
			if(last == true){
			char* bits;
			char* space = new char[PAGE_SIZE];
			char* recSpace = new char[PAGE_SIZE];
			//bits = NULL;
			int length = t.length();
			int currentPosInRec = sizeof (int) * (7);
			for(int i = 0; i < length; i++){
			//int k = 0;
				//cout<<i<<endl;
				space[i] = t[i];
				//cout<<space[i]<<endl;
			//}
			//length++;
			((int *) recSpace)[1] = currentPosInRec;
			//((int *) recSpace)[3] = currentPosInRec;
			//space[length] = 0;
			//k++;
		}
		space[length] = 0;
			if(runningSum == Float){
				*(double *) &(recSpace[currentPosInRec]) = atof(space);
				currentPosInRec +=sizeof(double);
			}
			((int *) recSpace)[0] = currentPosInRec;
			//((int *) recSpace)[1] = currentPosInRec;
			bits = new char [currentPosInRec];
			memcpy(bits,recSpace,currentPosInRec);
			//delete [] space;
			//delete [] recSpace;
			Record newR;
			newR.Consume(bits);
			_record.Swap(newR);
				last = false;	
				return true;
			}
			else{
				//_record.Nullify();
				//oFile.open(s,ios::out);
				//oFile << t;
				//oFile.close();
				return false;
			}		
		}
	}
}

ostream& Sum::print(ostream& _os) {
	_os << "SUM {F:" << schemaIn << "}\n{" << schemaOut << "}\n" <<*producer<<"\n";	
	return _os;
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
  schemaIn = _schemaIn;
  schemaOut = _schemaOut;
  groupingAtts = _groupingAtts;
  compute = _compute;
  producer = _producer;

}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record)
{
//cout<<"GROUP BY"<<endl;
	while(producer->GetNext(_record))
	{
		if(first == true)
		{
			_r = _record;
			//_r.print(cout,schemaOut);
			double val;
			runningsum = compute.Apply(_r,intvalue,val);
			recmap.Insert(_r,val);
			first = false;
			recmap.MoveToStart();
		}
		else
		{
			while(!recmap.AtEnd())
			{
				_r = recmap.CurrentKey();
				if(groupingAtts.Run(_r,_record) == 0)
				{
					double sum = recmap.CurrentData();
					double val;
					runningsum = compute.Apply(_r,intvalue,val);
					sum+=val;
					recmap.CurrentData() = sum;
					found = true;
					recmap.MoveToFinish();
					//cout<<recmap.Length()<<endl;
					
				}
				if(found == false)
				{
					recmap.Advance();
				}
			}
			if(found == false)
			{
				_r = _record;
				double val;
				runningsum = compute.Apply(_r,intvalue,val);
				recmap.Insert(_r,val);
				//cout<<recmap.Length()<<endl;
				
			}
			else
			{
				found = false;
			}
			recmap.MoveToStart();
		}
	}
	if(producer->GetNext(_record) != true)
	{
		int counter = 0;
		vector<Attribute> atts = schemaOut.GetAtts();
		if(last == true)
		{
			recmap.MoveToStart();
		}
		if(recmap.AtEnd() != true)
		{
			_r = recmap.CurrentKey();
			float f;
			string s;
			int in;
			char* ri = _r.GetColumn(1);
			/*if(atts[1].type == INTEGER)
			{
				string s(ri);
				in = atoi(ri);
				cout << in << endl;
			}
			else if(atts[1].type == FLOAT)
			{
				string s(ri);
				f = stof(s);
				cout << f << endl;
			}
			else if(atts[1].type == STRING)
			{
				s = ri;
			}*/
			//cout << ri << endl;
			double data = recmap.CurrentData();
			string info = to_string(data);
			recmap.Advance();
			counter++;
			char* bits;
			char* space = new char[PAGE_SIZE];
			char* recSpace = new char[PAGE_SIZE];
			int l = schemaOut.GetNumAtts();
			int currentPosInRec = sizeof (int) *(l + 1);
			int length = info.length();//changed here
			for(int i = 0; i < length; i++){
				space[i] = info[i];
				((int *) recSpace)[1] = currentPosInRec;
			}
			for(int i = 1; i < l; i++)//from Records.cc 
			{
								// set up the pointer to the current attribute in the record
				((int *) recSpace)[i + 1] = currentPosInRec;

				// null terminate the string
				space[length] = 0;

				length++;
				// then we convert the data to the correct binary representation
				if (atts[i].type == Integer) {
					*((int *) &(recSpace[currentPosInRec])) = atoi (space);	
					currentPosInRec += sizeof (int);
				}

				else if (atts[i].type == Float) {
					*((double *) &(recSpace[currentPosInRec])) = atof (space);
					currentPosInRec += sizeof (double);
				}
				/*else if (atts[i].type == String) {
					// align things to the size of an integer if needed
					if (length % sizeof (int) != 0) {
						length += sizeof (int) - (length % sizeof (int));
					}

					strcpy (&(recSpace[currentPosInRec]), space); 
					currentPosInRec += length;
				}*/
			}
			bits = new char [currentPosInRec];
			memcpy(bits,recSpace,currentPosInRec);
			Record newR;
			newR.Consume(bits);
			//newR.print(cout,schemaOut);
			//cout << "SIZE OF NEWR " << newR.GetSize() << endl;
			//cout << "SIZE OF _R " << _r.GetSize() << endl;
			lastR->AppendRecords(newR,_r,1,l);
			//cout << "RESULT: " << endl;
			last = false;
			_record.Swap(*lastR);
			return true;
		}
		else{
		return false;
		}
	}
}

ostream& GroupBy::print(ostream& _os) {
	_os << "GROUP BY {SCHEMAIN:" << schemaIn << "}\n{SCHEMAOUT:" << schemaOut << "}\n{GROUPINGATTS:" << groupingAtts << "}\n" << "}\n{PROD:" << *producer << "}\n";	
	return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
  schema = _schema;
  outFile = _outFile;
  producer = _producer;
  oFile.open(outFile,ios::out);
}

WriteOut::~WriteOut() {

  oFile.close();
}

ostream& WriteOut::print(ostream& _os) {
	_os << "WRITEOUT: {" << schema << "}\n{" << outFile << "}\n{PRODUCER: " << *producer << "}\n";
		
	return _os;
}

bool WriteOut::GetNext(Record& _record)
{
	//cout << "WRITEOUT" << endl;
	bool ret = producer->GetNext(_record);
	if(true == ret)
	{
		_record.print(oFile,schema);//print to file
		return true;
	}
	else
	{
		return false;
	}
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {	
	//_os << "QUERY EXECUTION TREE: \n" << endl;//
	//_os << *_op.root << endl;
		
	
	return _os;//<< "QUERY EXECUTION TREE: \n" << *_op.root;
}
