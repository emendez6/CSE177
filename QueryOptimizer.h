#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>

using namespace std;


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	bool calcminjoin(vector<string> _keeptbls, vector<unsigned int> _temptuples,OptimizationTree* _root,vector<string> _att);
	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);
	bool findcondition(vector<string> _tblname,vector<unsigned int> _tuple ,OptimizationTree* _root, AndList* _predicate, int _count);
	bool findcondition(vector<string> _tblname,vector<unsigned int> _tuple ,OptimizationTree* _root, AndList* _predicate);
};

#endif // _QUERY_OPTIMIZER_H
