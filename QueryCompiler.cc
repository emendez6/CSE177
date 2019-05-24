#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include <cmath>
#include<iostream>
#include<fstream>
#include <bits/stdc++.h>
using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	NameList* _Groupingatts = new NameList;
	_Groupingatts = _groupingAtts;
	int nTbl = 0;
	int idx = 0;
	int count = 0;
	int count2 = 0;
	unsigned int tuples;
	unsigned int newtp;
	unsigned int distnum;
	unsigned int tpl1 = 1, tpl2 = 1, z;
	unsigned int temp = 1;
	Record scan;
	vector<string> tbl;
	vector<string> att;
	AndList* pred = new AndList;
	pred =  _predicate;
	vector<int> tpl;
	vector<unsigned int> num;
	map<string,RelationalOp*>ops;
	CNF cnf;

	for (TableList* node = _tables; node != NULL; node = node->next) nTbl += 1;//get number of tables
	Schema* forestSchema = new Schema[nTbl];

//--------------------------------------------------------------------------------------------------------------------------------SCAN
	for (TableList* node = _tables; node != NULL; node = node->next){

		string s = node->tableName;
		bool b = catalog->GetSchema(s, forestSchema[idx]);
		if (false == b) {
			cout << "Semantic error: table " << s << " does not exist in the database!" << endl;
			exit(1);
		}

		DBFile dbFile;// = new DBFile;
		RelationalOp* scan = new Scan(forestSchema[idx],dbFile,s);
		ops.insert({s,scan});
		
		idx++;
	}

//--------------------------------------------------------------------------------------------------------------------------------SCAN


//--------------------------------------------------------------------------------------------------------------------------------SELECT
	
	for (TableList* node = _tables; node != NULL; node = node->next)
	{
		string n = node->tableName;
		tbl.push_back(n);
	}

	for (int i = 0; i < nTbl; i++) {
		int l1 = 0;
		Record literal;
		if(cnf.ExtractCNF(*pred, forestSchema[i], literal) == 0)
		{
			catalog->GetNoTuples(tbl[i], tuples);
			tpl.push_back(tuples);
			for(int j = 0; j <cnf.numAnds ;j++)
			{
				int num1 = cnf.andList[j].whichAtt1;
				if (cnf.andList[j].op == LessThan || cnf.andList[j].op == GreaterThan)
				{
					count = 1;
					num.push_back(pow(3,count));
					for(int x = 0; x <num.size();x++)
					{
						tpl1 = num[x];
					}
					z = tpl[i] / tpl1;
					tpl[i]= z;
				}
				if(cnf.andList[j].op == Equals)
				{
					catalog->GetAttributes(tbl[i],att);
					catalog->GetNoDistinct(tbl[i],att[num1],distnum);
					count2++;
					for(int x = 0; x <num.size();x++)
					{
						tpl2 = num[x];
					}
					if(count2 == 1)
					{
						tpl2 = 1;
						temp = temp*distnum;
					}
					tpl1 = temp * tpl2;
					z = tpl[i] / distnum;
					tpl[i]= z;
				}	
				j++;
				int s = literal.GetSize();
				auto it = ops.find(tbl[i]);
				Schema schemaS;
				catalog->GetSchema(tbl[i],schemaS);
				RelationalOp* select = new Select(schemaS, cnf, literal, it->second);
				it->second = select;
			}
		}
	}
//----------------------------------------------------------------------------------------------------------------------------------SELECT
	//cout << "NO SEG" << endl;
//--------------------------------------------------------------------------------------------------------------------------------OPTIMIZE
	OptimizationTree* root = new OptimizationTree;

	for (int tb = 0; tb < tbl.size(); tb++){
		root->tables.push_back(tbl[tb]);
	}
	for (int t = 0; t < tpl.size(); t++){
		root->tuples.push_back(tpl[t]);
	}
	optimizer->Optimize(_tables, _predicate, root);

//-------------------------------------------------------------------------------------------------------------------------------OPTIMIZE
	
	Schema* schemal = new Schema[root->tables.size()];
	Schema* schemar = new Schema[root->tables.size()];
	Schema* schemasout = new Schema[root->tables.size()];
	Schema schemajoin;
	int joinCount = 0;

//-------------------------------------------------------------------------------------------------------------------------------JOIN	
	bool join = false;
	if(root->tables.size() != 1)
	{
		join = true;
	}
	if(join == true)
	{	
		for(int i = 0; i < root->tables.size() - 1; i++)
		{
			if(i == 0)
			{
				string st = root->tables[i];
				catalog->GetSchema(st,schemal[i]);
				string r = root->tables[i + 1];
				catalog->GetSchema(r,schemar[i]);
				catalog->GetSchema(st,schemasout[i]);
				schemasout->Append(schemar[i]);
				joinCount++;
			}
			if(i >= 1)
			{
				schemal[i] = schemasout[i - 1];
				schemasout[i] = schemasout[i - 1];
				string rt = root->tables[i + 1];
				catalog->GetSchema(rt,schemar[i]);
	
			catalog->GetSchema(rt,schemasout[i]);
				joinCount++;
				
			}
		}
		schemajoin.Append(schemasout[root->tables.size()-2]);

		int c;
		int c2;
		int left;
		int right;
		for(int i  = 0; i < root->tables.size() - 1; i++)
		{
			CNF _cnf;
			_cnf.ExtractCNF(*_predicate,schemal[i],schemar[i]);
			auto it = ops.find(root->tables[i]);
			auto it2 = ops.find(root->tables[i+1]);
			RelationalOp* join = new Join(schemal[i],schemar[i],schemasout[i],_cnf,it->second,it2->second);
			ops.erase(it->first);
			it2->second = join;
			
		}
	}

//-----------------------------------------------------------------------------------------------------------------------------------JOIN

// create the remaining operators based on the query

	Schema* schemain = new Schema[root->tables.size()];
	Schema* schemaout = new Schema[root->tables.size()];
	vector<string> sumName;
	vector<string> typeSum;
	vector<unsigned int> distinctSum;
	string sm = "SUM";
	sumName.push_back(sm);
	string type = "FLOAT";
	typeSum.push_back(type);
	distinctSum.push_back(1);
	Schema* sch = new Schema(sumName,typeSum,distinctSum);
	//forestSchema->Append(*sch);
//-------------------------------------------------------------------------------------------------------------------------------SUM
	if(_finalFunction != NULL && _Groupingatts == NULL)
	{
		Function funct;
		int r;
		double results;
		if(joinCount == 0)
		{
			auto it = ops.begin();
			funct.GrowFromParseTree(_finalFunction,*forestSchema);
			RelationalOp* sum = new Sum(*forestSchema,*sch,funct,it->second);
			it->second = sum;
		}
		else{
			auto it = ops.begin();
			funct.GrowFromParseTree(_finalFunction, schemajoin);
			RelationalOp* sum = new Sum(schemajoin,*sch,funct,it->second);
			it->second = sum;
		}
	}
//---------------------------------------------------------------------------------------------------------------------------------SUM

//---------------------------------------------------------------------------------------------------------------------------------PROJECT
	vector<int> atts;
	int x;
	Record project;	
	if(_Groupingatts == NULL &&_finalFunction == NULL)
	{
			for(int i = 0; i < root->tables.size(); i++)
			{
				string s = root->tables[i];
				//cout<<s<<endl;
				catalog->GetSchema(s, schemain[i]);
				catalog->GetSchema(s, schemaout[i]);
				catalog->GetAttributes(s,att);
				if(i > 0){
					schemain->Append(schemain[i]);
					schemaout->Append(schemaout[i]);

				}
				
			}
			//cout<<*schemain<<endl;
			//cout<<*schemaout<<endl;
			for (NameList* node = _attsToSelect; node != NULL; node = node->next)
			{
				string a = node->name;
				x = schemain->Index(a);
				//cout<<a<<endl;
				//cout<<x<<endl;
				atts.push_back(x);
			}
			sort(atts.begin(),atts.end());
			schemaout->Project(atts);
			int numattin = att.size();
			int numatsout = atts.size();
			int* keepme = new int[atts.size()];
			auto it = ops.begin();
			for(int i = 0; i < atts.size(); i++)
			{
				keepme[i] = atts[i];
				//cout<<keepme[i]<<endl;
			}
			if(join == false){
				RelationalOp* proj = new Project(*schemain,*schemaout,numattin,numatsout,keepme,it->second);
				//cout << *proj << endl;
				it->second = proj;
			}
			else{
				/*cout<<"SCHEMA OUT{"<<*schemaout<<endl;
				cout<<"}DONE"<<endl;
				cout<<numattin<<endl;
				cout<<"numatsout"<<numatsout<<endl;*/
				RelationalOp* proj = new Project(schemajoin,*schemaout,numattin,numatsout,keepme,it->second);
				//cout << *proj << endl;
				it->second = proj;
			}
	}
//------------------------------------------------------------------------------------------------------------------------------PROJECT

	Schema distschema;
//------------------------------------------------------------------------------------------------------------------------------DISTINCT
	
	if(_distinctAtts >= 1)
	{

		if(join == false)
		{			
			auto it = ops.begin();
			distschema.Append(*schemaout);
			RelationalOp* dist = new DuplicateRemoval(distschema,it->second);
			it->second = dist;
		}
		else
		{
			auto it = ops.begin();
			distschema.Append(*schemaout);
			RelationalOp* dist = new DuplicateRemoval(distschema,it->second);
			//distschema.Append(schemajoin);
			it->second = dist;
		}
	
	}

//------------------------------------------------------------------------------------------------------------------------------DISTINCT
	
	
	Function _compute;
	vector<string> attsname;
	attsname.push_back(sm);
	vector<unsigned int> attdist;
	vector<int> attsIndex;
	attdist.push_back(1);
	vector<string> attTypes;
	attTypes.push_back(type);
	FuncOperator* func = new FuncOperator;
	func = _finalFunction;
	NameList* groupingatts = new NameList;
	groupingatts = _groupingAtts;
	Schema* schemaAttsout;
	Schema* sumSchema;
	int* ga;
	int ganum = 0;
//------------------------------------------------------------------------------------------------------------------------------GROUP BY
	if(_Groupingatts != NULL)
	{

		while(_groupingAtts != NULL)//get the attributes to group by
		{
			char* attname = _groupingAtts->name;
			attsname.push_back(attname);
			_groupingAtts = _groupingAtts->next;
			ganum++;
			
		}
		if(join == false)
		{		
			string s = root->tables[root->tables.size()-1];
			catalog->GetSchema(s,*schemasout);//get correct schema
			//cout << *schemasout << endl;
			for(int i =1; i < attsname.size(); i++)//get the number of distinct and the types
			{
				unsigned int dis = schemasout->GetDistincts(attsname[i]);
				attdist.push_back(dis);
				int attindex = schemasout->Index(attsname[i]);
				attsIndex.push_back(attindex);
				ga = &attindex;
				//cout << attindex << endl;
				//cout << dis << endl;
				if(schemasout->FindType(attsname[i]) == 0)
				{
					attTypes.push_back("INTEGER");
				}
				if(schemasout->FindType(attsname[i]) == 1)
				{
					attTypes.push_back("FLOAT");
				}
				if(schemasout->FindType(attsname[i]) == 2)
				{
					attTypes.push_back("STRING");
				}
			}

			_compute.GrowFromParseTree(_finalFunction,*schemasout);
			schemaAttsout = new Schema(attsname,attTypes,attdist);//make into schema
			//schemaAttsout->Project(attsIndex);
			//cout << *schemaAttsout << endl;
			OrderMaker orderMaker(*schemaAttsout,ga,ganum);
			auto it = ops.begin();
			RelationalOp* groupBy = new GroupBy(*schemasout,*schemaAttsout,orderMaker,_compute,it->second);
			it->second = groupBy;
		}
		else
		{
			//string s = root->tables[root->tables.size()-1];
			//catalog->GetSchema(s,*schemasout);//get correct schema
			//cout << *schemasout << endl;
			for(int i =1; i < attsname.size(); i++)//get the number of distinct and the types
			{
				unsigned int dis = schemajoin.GetDistincts(attsname[i]);
				attdist.push_back(dis);
				int attindex = schemajoin.Index(attsname[i]);
				//cout << attindex << endl;
				attsIndex.push_back(attindex);
				ga = &attindex;
				//cout << attsname[i] << endl;
				//cout << attindex << endl;
				//cout << dis << endl;
				if(schemasout->FindType(attsname[i]) == 0)
				{
					attTypes.push_back("INTEGER");
					//cout << "HEEEY" << endl;
				}
				if(schemasout->FindType(attsname[i]) == 1)
				{
					//cout << "HEY" << endl;
					attTypes.push_back("FLOAT");
				}
				if(schemasout->FindType(attsname[i]) == 2)
				{
					attTypes.push_back("STRING");
				}
			}
			_compute.GrowFromParseTree(_finalFunction,schemajoin);
			schemaAttsout = new Schema(attsname,attTypes,attdist);//make into schema
			//cout <<"SCHEMAAAAAA" <<  *schemaAttsout << endl;
			//schemaAttsout->Project(attsIndex);
			
			OrderMaker orderMaker(*schemaAttsout,ga,ganum);
			auto it = ops.begin();
			RelationalOp* groupBy = new GroupBy(schemajoin,*schemaAttsout,orderMaker,_compute,it->second);
			it->second = groupBy;
		}
}
//-----------------------------------------------------------------------------------------------------------------------------GROUP BY

//-----------------------------------------------------------------------------------------------------------------------------WRITEOUT
	string s = "output.txt";
	if(_finalFunction != NULL && _Groupingatts == NULL)//SUM
	{
		//cout<<"HERE"<<endl;
		auto it = ops.begin();
		RelationalOp* writeout = new WriteOut(*sch,s,it->second);
		//cout << *writeout << endl;
		_queryTree.SetRoot(*writeout);
		//cout << *writeout << endl;
	
	}
	else if(_Groupingatts != NULL)//GROUPBY
	{
		//cout<<"HERE2"<<endl;
		auto it = ops.begin();
		RelationalOp* writeout = new WriteOut(*schemaAttsout,s,it->second);
		_queryTree.SetRoot(*writeout);
		cout << *writeout << endl;
	}
	else if(_distinctAtts >= 1)//Distinct
	{
		//cout<<"HERE3"<<endl;
		auto it = ops.begin();
		//cout<<distschema<<endl;//wrong schema
		RelationalOp* writeout = new WriteOut(distschema,s,it->second);
		_queryTree.SetRoot(*writeout);
		//cout << *writeout << endl;

	}
	else//Simple Select
	{
		//cout<<"HERE4"<<endl;
		auto it = ops.begin();
		//cout<<*schemaout<<endl;
		WriteOut* writeout = new WriteOut(*schemaout,s,it->second);
		_queryTree.SetRoot(*writeout);
		//cout << *writeout << endl;
	}
}
