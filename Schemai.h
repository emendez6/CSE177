#ifndef _SCHEMAI_H
#define _SCHEMAI_H

#include <string>
#include <vector>
#include <iostream>
#include "Config.h"

using namespace std;


/* Data structure for schema attributes:
 * name of attribute
 * type of attribute
 * number of distinct values
 */
class Attributei {
public:
	string name;
	Type type;
	unsigned int noDistinct;
	int index; //added

	// constructors and destructor
	Attributei();
	Attributei(const Attributei& _other);
	Attributei& operator=(const Attributei& _other);
	void Swap(Attributei& _other);

	virtual ~Attributei() {}
};


/* Class to manage schema of relations:
 * materialized on disk
 * intermediate result during query execution
 */
class Schemai {
private:
	// attributes in schema
	vector<Attributei> atts;

public:
	// default constructor
	Schemai() {}
	// full constructor
	Schemai(vector<string>& _attributes,	vector<string>& _attributeTypes,
	vector<unsigned int>& _distincts, vector<int>&_index);
	// copy constructor
	Schemai(const Schemai& _other);
	// assignment operator
	Schemai& operator=(const Schemai& _other);
	// swap function
	void Swap(Schemai& _other);

	// destructor
	virtual ~Schemai() {atts.clear();}

	// get functions
	unsigned int GetNumAtts() {return atts.size();}
	vector<Attributei>& GetAtts() {return atts;}

	// append other schema
	int Append(Schemai& _other);

	int Sort(Schemai& _other);//------------------------------------Modification

	// find index of specified attribute
	// return -1 if attribute is not present
	int Index(string& _attName);

	// find number of distincts of specified attribute
	// return -1 if attribute is not present
	int GetDistincts(string& _attName);

	// rename an attribute
	int RenameAtt(string& _oldName, string& _newName);

	// project attributes of a schema
	// only attributes indexed in the input vector are kept after projection
	// index begins from 0
	// return -1 if failure, 0 otherwise
	int Project(vector<int>& _attsToKeep);

	// find type of the specified attribute
	// return arbitrary type if attribute is not present
	// call only after Index returns valid result
	Type FindType(string& _attName);

	void Clear();

	// operator for printing
	friend ostream& operator<<(ostream& _os, Schemai& _c);
};

#endif //_SCHEMAI
