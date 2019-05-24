#include <iostream>
#include "Config.h"
#include "Swap.h"
#include "Schemai.h"
#include <bits/stdc++.h>
#include <algorithm>

using namespace std;


Attributei::Attributei() : name(""), type(Name), noDistinct(0) , index(0){}// added

Attributei::Attributei(const Attributei& _other) :
	name(_other.name), type(_other.type), noDistinct(_other.noDistinct), index(_other.index){}// added

Attributei& Attributei::operator=(const Attributei& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	name = _other.name;
	type = _other.type;
	noDistinct = _other.noDistinct;
	index = _other.index;//added

	return *this;
}

void Attributei::Swap(Attributei& _other) {
	STL_SWAP(name, _other.name);
	SWAP(type, _other.type);
	SWAP(noDistinct, _other.noDistinct);
	SWAP(index,_other.index)//added
}


Schemai::Schemai(vector<string>& _attributes,	vector<string>& _attributeTypes,
	vector<unsigned int>& _distincts, vector<int>&_index) {//added
	

	for (int i = 0; i < _attributes.size(); i++) {
		Attributei a;
		a.name = _attributes[i];
		a.noDistinct = _distincts[i];
		a.index = _index[i];
		if (_attributeTypes[i] == "INTEGER") a.type = Integer;
		else if (_attributeTypes[i] == "FLOAT") a.type = Float;
		else if (_attributeTypes[i] == "STRING") a.type = String;
		
		atts.push_back(a);
	}


}

Schemai::Schemai(const Schemai& _other) {

	for (int i = 0; i < _other.atts.size(); i++) {
		Attributei a; a = _other.atts[i];
		atts.push_back(a);
	}
}

Schemai& Schemai::operator=(const Schemai& _other) {
	// handle self-assignment first
	if (this == &_other) return *this;

	for (int i = 0; i < _other.atts.size(); i++) {
		Attributei a; a = _other.atts[i];
		atts.push_back(a);
	}

	return *this;
}

void Schemai::Swap(Schemai& _other) {
	atts.swap(_other.atts);
}

int Schemai::Append(Schemai& _other) {
	for (int i = 0; i < _other.atts.size(); i++) {
		int pos = Index(_other.atts[i].name);
		if (pos != -1) return -1;
	}

	for (int i = 0; i < _other.atts.size(); i++) {
		Attributei a; a = _other.atts[i];
		atts.push_back(a);
	}

	return 0;
}
//--------------------Modification
int Schemai::Sort(Schemai& _other)
{
	sort(_other.atts.begin(),_other.atts.end(),[ ]( const Attributei& a, const Attributei& b) {return a.index < b.index;});
}
//--------------------
int Schemai::Index(string& _attName) {
	for (int i = 0; i < atts.size(); i++) {
		if (_attName == atts[i].name) return i;
	}

	// if we made it here, the attribute was not found
	return -1;
}
//--------ADDED-----------





//---------------------
Type Schemai::FindType(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return Integer;

	return atts[pos].type;
}

int Schemai::GetDistincts(string& _attName) {
	int pos = Index(_attName);
	if (pos == -1) return -1;

	return atts[pos].noDistinct;
}

int Schemai::RenameAtt(string& _oldName, string& _newName) {
	int pos = Index(_newName);
	if (pos != -1) return -1;

	pos = Index(_oldName);
	if (pos == -1) return -1;


	atts[pos].name = _newName;

	return 0;
}

int Schemai::Project(vector<int>& _attsToKeep) {
	int numAttsToKeep = _attsToKeep.size();
	int numAtts = atts.size();
	
	// too many attributes to keep
	if (numAttsToKeep > numAtts) return -1;

	vector<Attributei> copy; atts.swap(copy);

	for (int i=0; i<numAttsToKeep; i++) {
		int index = _attsToKeep[i];
		if ((index >= 0) && (index < numAtts)) {
			Attributei a; a = copy[index];
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

void Schemai::Clear() {
	atts.clear();
}


ostream& operator<<(ostream& _os, Schemai& _c) {
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
		_os << " INDEX[" << _c.atts[i].index << "]";
		if (i < _c.atts.size()-1) _os << ", \n";
	}
	_os << ")";

	return _os;
}
