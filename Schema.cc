#include <iostream>
#include "Config.h"
#include "Swap.h"
#include "Schema.h"
#include <bits/stdc++.h>
#include <algorithm>

using namespace std;


Attribute::Attribute() : name(""), type(Name), noDistinct(0) , index(0){}// added

Attribute::Attribute(const Attribute& _other) :
	name(_other.name), type(_other.type), noDistinct(_other.noDistinct), index(_other.index){}// added

Attribute& Attribute::operator=(const Attribute& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	name = _other.name;
	type = _other.type;
	noDistinct = _other.noDistinct;
	index = _other.index;//added

	return *this;
}

void Attribute::Swap(Attribute& _other) {
	STL_SWAP(name, _other.name);
	SWAP(type, _other.type);
	SWAP(noDistinct, _other.noDistinct);
	SWAP(index,_other.index)//added
}


Schema::Schema(vector<string>& _attributes,	vector<string>& _attributeTypes,
	vector<unsigned int>& _distincts) {//added
	
	for (int i = 0; i < _attributes.size(); i++) {
		Attribute a;
		a.name = _attributes[i];
		a.noDistinct = _distincts[i];
		if (_attributeTypes[i] == "INTEGER") a.type = Integer;
		else if (_attributeTypes[i] == "FLOAT") a.type = Float;
		else if (_attributeTypes[i] == "STRING") a.type = String;
		
		atts.push_back(a);
	}


}

/*Schema::Schema(vector<string>& _attributes,	vector<string>& _attributeTypes,
	vector<unsigned int>& _distincts, vector<int>&_index) {//added
	
	for (int i = 0; i < _attributes.size(); i++) {
		Attribute a;
		a.name = _attributes[i];
		a.noDistinct = _distincts[i];
		a.index = _index[i];
		if (_attributeTypes[i] == "INTEGER") a.type = Integer;
		else if (_attributeTypes[i] == "FLOAT") a.type = Float;
		else if (_attributeTypes[i] == "STRING") a.type = String;
		
		atts.push_back(a);
	}


}
*/
Schema::Schema(const Schema& _other) {

	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}
}

Schema& Schema::operator=(const Schema& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}

	return *this;
}

void Schema::Swap(Schema& _other) {
	atts.swap(_other.atts);
}

int Schema::Append(Schema& _other) {
	for (int i = 0; i < _other.atts.size(); i++) {
		int pos = Index(_other.atts[i].name);
		if (pos != -1) return -1;
	}

	for (int i = 0; i < _other.atts.size(); i++) {
		Attribute a; a = _other.atts[i];
		atts.push_back(a);
	}

	return 0;
}

/*int Schema::Sort(Schema& _c, Catalog _c)
{
	sort(_c.atts.begin(),_c.atts.end(),[ ]( const Attribute& a, const Attribute& b) {return a.index < b.index;});
	
}*/

int Schema::Index(string& _attName) {
	for (int i = 0; i < atts.size(); i++) {
		if (_attName == atts[i].name) return i;
	}

	// if we made it here, the attribute was not found
	return -1;
}
//--------ADDED-----------





//---------------------
Type Schema::FindType(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return Integer;

	return atts[pos].type;
}

int Schema::GetDistincts(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return -1;

	return atts[pos].noDistinct;
}

int Schema::RenameAtt(string& _oldName, string& _newName) {
	int pos = Index(_newName);
	if (pos != -1) return -1;

	pos = Index(_oldName);
	if (pos == -1) return -1;


	atts[pos].name = _newName;

	return 0;
}

int Schema::Project(vector<int>& _attsToKeep) {
	int numAttsToKeep = _attsToKeep.size();
	int numAtts = atts.size();
	
	// too many attributes to keep
	if (numAttsToKeep > numAtts) return -1;

	vector<Attribute> copy; atts.swap(copy);

	for (int i=0; i<numAttsToKeep; i++) {
		int index = _attsToKeep[i];
		if ((index >= 0) && (index < numAtts)) {
			Attribute a; a = copy[index];
			atts.push_back(a);
		}
		else {
			atts.swap(copy);
			copy.clear();

			return -1;
		}
	}

	copy.clear();

	return 0;
}

void Schema::Clear() {
	atts.clear();
}


ostream& operator<<(ostream& _os, Schema& _c) {
	_os << "(";
	for(int i=0; i<_c.atts.size(); i++) {
		_os << _c.atts[i].name << ':';

		switch(_c.atts[i].type) {
			case Integer:
				_os << "INTEGER";
				break;
			case Float:
				cout << "FLOAT";
				break;
			case String:
				cout << "STRING";
				break;
			default:
				cout << "UNKNOWN";
				break;
		}

		_os << " [" << _c.atts[i].noDistinct << "]";
		//_os << " [" << _c.atts[i].index << "]";
		if (i < _c.atts.size()-1) _os << ", \n";
	}
	_os << ")";

	return _os;
}
