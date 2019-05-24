#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}
bool QueryOptimizer::findcondition(vector<string> _tblname, vector<unsigned int>_tuple,OptimizationTree* _root, AndList* _predicate,int _count)
{
	Schema* _forestSchema = new Schema[_tblname.size()];//schema for tables that are left
	Schema* _rootforestSchema = new Schema[_root->tables.size()];//schema for root
	AndList* predicate = new AndList;
	AndList* pred = new AndList;
	vector<string> atts;
	vector<string> _keeptbls;
	vector<unsigned int> _keeptuples;
	CNF cnf;
	int index = 0;
	predicate = _predicate;
	pred = _predicate;
	string l;
	string r;

	for(int i = 0; i < _root->tables.size(); i++)//get tables with schema
	{
		catalog->GetSchema(_root->tables[i], _rootforestSchema[i]);
	}
	index = _root->tables.size()-1;
	for(int i = 0; i < _tblname.size(); i++)//get tables with schema
	{
		catalog->GetSchema(_tblname[i], _forestSchema[i]);
	}
	for(int i = _tblname.size()-1; i >= 0; i--)
	{
		predicate = pred;
		for(int e = 0; e < _count; e++)
		{
			if(cnf.ExtractCNF(*predicate,_forestSchema[i],_rootforestSchema[index]) == 0)//dont create a cnf for min table
			{
				l = predicate->left->left->value;//save the attributes
				r = predicate->left->right->value;
				//cout << l << " and " << r << endl;
				int m = _forestSchema[i].Index(l);
				//cout << _forestSchema[i] << endl;
				//cout << _rootforestSchema[index] << endl;
				int n = _rootforestSchema[index].Index(r);
				int m1 = _forestSchema[i].Index(r);
				int n1 = _rootforestSchema[index].Index(l);
				//cout << m << " " << n << " " << m1 << " " << n1 << endl;
				if((_forestSchema[i].Index(l) != -1 && _rootforestSchema[index].Index(r) != -1) ||(_rootforestSchema[index].Index(l) != -1 && _forestSchema[i].Index(r) != -1) )//find if the tables have a condition with the min table
				{
					//cout << l << " " << r << _tblname[i] << endl;
					_keeptbls.push_back(_tblname[i]);//save the tables that do
					_keeptuples.push_back(_tuple[i]);//along with their tuples
					atts.push_back(l);//and the atrributes
					atts.push_back(r);
					_tuple[i] = _tuple.back();
					_tuple.pop_back();
					
				}
				predicate = predicate->rightAnd;//increment to next predicate
			}
		}
	}
	if(_keeptbls.size() != 0)
	{
		calcminjoin(_keeptbls,_keeptuples,_root,atts);//calculate the estimation
	}
	for(int i = 0; i < _root->tables.size(); i++)//delete tables that are now root
	{
		_tblname.erase(remove(_tblname.begin(), _tblname.end(),_root->tables[i]),_tblname.end());
	}
	if(_tblname.size() > 1)//if there are still tables left then find next condition
	{		
		findcondition(_tblname,_tuple,_root,_predicate,_count);
	}
	if(_tblname.size() == 1)
	{
		findcondition(_tblname,_tuple,_root,_predicate);
	}
	else
	{
		return false;
	}
}
bool QueryOptimizer::findcondition(vector<string> _tblname, vector<unsigned int>_tuple,OptimizationTree* _root, AndList* _predicate)
{
	Schema* _forestSchema = new Schema[_tblname.size()];//schema for tables that are left
	Schema* _rootforestSchema = new Schema[_root->tables.size()];//schema for root
	AndList* predicate = new AndList;
	vector<string> atts;
	vector<string> _keeptbls;
	vector<unsigned int> _keeptuples;
	CNF cnf;
	int index = 0;
	predicate = _predicate;
	string l;
	string r;
	for(int i = 0; i < _root->tables.size(); i++)//get tables with schema
	{
		catalog->GetSchema(_root->tables[i], _rootforestSchema[i]);
	}
	index = _root->tables.size() - 1;
	for(int i = 0; i < _tblname.size(); i++)//get tables with schema
	{
		catalog->GetSchema(_tblname[i], _forestSchema[i]);
	}
	for(int i = _tblname.size() - 1; i >= 0; i--)
	{

		if(cnf.ExtractCNF(*predicate,_forestSchema[i],_rootforestSchema[index]) == 0)//dont create a cnf for min table
		{
			l = predicate->left->left->value;//save the attributes
			r = predicate->left->right->value;
			if((_forestSchema[i].Index(l) != -1 && _rootforestSchema[index].Index(r) != -1) ||(_rootforestSchema[index].Index(l) != -1 && _forestSchema[i].Index(r) != -1) )//find if the tables hace a condition with the min table
			{
				//cout << l << " " << r << _tblname[i] << endl;
				_keeptbls.push_back(_tblname[i]);//save the tables that do
				_keeptuples.push_back(_tuple[i]);//along with their tuples
				atts.push_back(l);//and the atrributes
				atts.push_back(r);
				_tuple[i] = _tuple.back();
				_tuple.pop_back();
				
			}
			predicate = predicate->rightAnd;//increment to next predicate
		}
	}
	if(_keeptbls.size() != 0)
	{
		calcminjoin(_keeptbls,_keeptuples,_root,atts);//calculate the estimation
	}

	else
	{
		return false;
	}
	
}
bool QueryOptimizer::calcminjoin(vector<string> _keeptbls, vector<unsigned int> _keeptuples,OptimizationTree* _root,vector<string> _att)
{
	vector<string> _atts;//save all the attributes of our root table
	unsigned int dis;
	vector<unsigned int> dist;
	int tuples;
	bool found;//if attribute is found in first table
	int index = 0;//index of attributesstarting at 0

	for(int i = 0; i < _root->tables.size(); i++)
	{
		if(i == _root->tables.size()-1)//most recent root
		{
			catalog->GetAttributes(_root->tables[i],_atts);//get attributes
			for(int e = 0; e < _atts.size() ; e++)
			{
				if(_atts[e] == _att[index])//check if the attribute is in this table
				{

					found = true;
					break;
					
				}
				else//otherwise its in the second table
				{
					found = false;
				}
			}
			if(found == true)
			{
					catalog->GetNoDistinct(_root->tables[i],_att[index],dis);//get number of distincts
					dist.push_back(dis);//save value in vector
					index++;//increment index to next attribute
					break;
			}
			else
			{
					index++;//increment index
					catalog->GetNoDistinct(_root->tables[i],_att[index],dis);//get number of distinct
					dist.push_back(dis);//save value in vector
					break;
			}
		}
	}

	for(int i = 0; i < _keeptbls.size(); i++)//now check remaining tables
	{
		int min = *min_element(_keeptuples.begin(),_keeptuples.end());//find table with min
		if(_keeptuples[i] == min)//if the table is the min
		{
			if(found == true)
			{
				_root->tables.push_back(_keeptbls[i]);//save table as new root
				_keeptbls.erase(_keeptbls.begin()+i);//remove from keep tbls
				catalog->GetNoDistinct(_keeptbls[i],_att[index],dis);//get number of distinct
				dist.push_back((uint64_t)dis);//save to vector
				tuples = _keeptuples[i];//save tuple number
				_keeptuples.erase(_keeptuples.begin()+i);//erase tuples from vector
				break;
			}
			else
			{
				index--;//got back to first attribute
				_root->tables.push_back(_keeptbls[i]);//save table as new root
				_keeptbls.erase(_keeptbls.begin()+i);//remove from keep tbls
				catalog->GetNoDistinct(_keeptbls[i],_att[index],dis);//get number of distincts
				dist.push_back((uint64_t)dis);//save value in vector
				tuples = _keeptuples[i];//save tuple number
				_keeptuples.erase(_keeptuples.begin()+i);//erase tuples from vector
				break;
		
			}
		}
	}

	uint64_t rt = _root->noTuples;//get tuples from root
	uint64_t max = *max_element(dist.begin(),dist.end());//find max distinct
	uint64_t newTuples = (rt * tuples) / max;//compute estimation
	_root->noTuples = newTuples;//set root tuples to new
	_root->tuples.push_back(newTuples);
	if(_keeptbls.size() != 0)//if there are still more tables then repeat
	{
		calcminjoin(_keeptbls,_keeptuples,_root,_att);
	}
	else
	{
		return false;
	}

}
void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	vector<string> tblname;//stores table names
	vector<string> atts;
	vector<unsigned int> tuples;
	int _index;
	vector<string> keeptbls;//stores tables to keep
	vector<unsigned int> keeptuples;
	AndList* predicate = new AndList;
	CNF cnf;
	int count = 0;
	predicate = _predicate;
	string l;
	string r;
	Schema* forestSchema = new Schema[_root->tables.size()];

	for(int i = 0; i < _root->tables.size(); i++)//get tables with schema
	{
		tblname.push_back(_root->tables[i]);
		catalog->GetSchema(tblname[i], forestSchema[i]);
		tuples.push_back(_root->tuples[i]);
	}
	_root->tables.clear();//clear root that was passed in order to store new
	_root->tuples.clear();
	int min = *min_element(tuples.begin(),tuples.end());//find the table with min number of tuples
	int index = 0;

	for(int i = 0; i < tblname.size(); i++)//find root
	{
		if(tuples[i] == min)
		{
			_root->tables.push_back(tblname[i]);
			_root->noTuples = tuples[i];
			tuples[i] = tuples.back();
			tuples.pop_back();
			index = i;
		}
	}
	//cout << "Min table: " << _root->tables[0] << endl;
	if(tblname.size() == 1)
	{
		_root->tuples.push_back(tuples[index]);
	}
	else
	{
		for(int i = tblname.size() - 1; i >= 0; i--)
		{
			if(((cnf.ExtractCNF(*predicate,forestSchema[i],forestSchema[index]) == 0)  ||(cnf.ExtractCNF(*predicate,forestSchema[index],forestSchema[i]) == 0)) && i != index)//dont create a cnf for min table
			{
				l = predicate->left->left->value;//save the attributes
				r = predicate->left->right->value;
				//cout << l << " " << r << tblname[i] << endl;
				if((forestSchema[i].Index(l) != -1 && forestSchema[index].Index(r) != -1) ||(forestSchema[index].Index(l) != -1 && forestSchema[i].Index(r) != -1) )//find if the tables hace a condition with the min table
				{
					//cout << l << " " << r << tblname[i] << endl;
					keeptbls.push_back(tblname[i]);//save the tables that do
					keeptuples.push_back(tuples[i]);//along with their tuples
					atts.push_back(l);//and the atrributes
					atts.push_back(r);
					tuples[i] = tuples.back();
					tuples.pop_back();
					
				}
				count ++;//count how many predicates are left
				predicate = predicate->rightAnd;//increment to next predicate
			}
		}	
	}
	if(keeptbls.size() != 0)
	{
		calcminjoin(keeptbls,keeptuples,_root,atts);//calculate the estimation
	}
	for(int i = 0; i < _root->tables.size(); i++)//delete tables that are now root
	{
		tblname.erase(remove(tblname.begin(), tblname.end(),_root->tables[i]),tblname.end());
		//count--;
	}
	if(tblname.size() > 1)//if there are still tables left then find next condition
	{		
		findcondition(tblname,tuples,_root,_predicate,count);
	}
	if(tblname.size() == 1)
	{
		findcondition(tblname,tuples,_root,_predicate);
	}
}
