#include <string>
#include "Catalog.h"
#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName(""), currPage(0) {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	path = f_path;
	type = f_type;
	currPage = 0;
	
	int ret = file.Open(0, path);
	return ret;
}

int DBFile::Open (char* f_path) {
	file.Open(1, f_path);

	//cout << "Opened: " << f_path << endl;
	//cout<<"Number of pages "<<file.GetLength()<<endl;
	currPage = 0;
	file.GetPage(page, currPage);

	//file.GetPage(page,currPage);
	//page.EmptyItOut();
	//cout<<"opened"<<endl;
	//check if file already created
	
}

void DBFile::Load (Schema& schema, char* textFile) {
	FILE* f = fopen(textFile, "rt");

	//Record rec;// = new Record;

	//cout<<page.GetFirst(rec)<<endl;
	
	//if(fopen(textFile,"r"))
	//{
	//	cout << "Opened File " << textFile << endl;
	//}
	//spage.GetFirst(rec);
	//MoveFirst();
	//cout<<currPage<<"p"<<endl;
	//page.Append(rec);
	//currPage++;
	//cout<<rec.ExtractNextRecord(schema,*f)<<endl;
/*
	while(true){
	Record rec;
		if(rec.ExtractNextRecord(schema,*f)){
			if(!page.Append(rec)){
				file.AddPage(page,currPage++);
				page.EmptyItOut();
				page.Append(rec);									
			}		
		} else	break;	
	}	
	file.AddPage(page,currPage++);
	page.EmptyItOut();
*/

	//int counter= 0;
	page.EmptyItOut();
	currPage = 0;

	Record rec;
	while(rec.ExtractNextRecord(schema, *f)){
		int a = page.Append(rec);
		if (a == 0) {
			file.AddPage(page, currPage++);
			page.EmptyItOut();
			page.Append(rec);
		}
	
	}

	file.AddPage(page,currPage++);
	page.EmptyItOut();

	fclose(f);
	//cout << "Closed file " << textFile << endl;

}

int DBFile::Close () {
	int ret = file.Close();
	return ret;
}

void DBFile::MoveFirst () {
	currPage = 0;
	page.EmptyItOut();
}

void DBFile::AppendRecord (Record& rec) {
	page.Append(rec);
}

int DBFile::GetNext (Record& rec) {
	//cout << "Get Next" << endl;
	int ret = page.GetFirst(rec);
	
	if (0 == ret){
		if(currPage == file.GetLength()-1){
			//cout<<"Number of pages "<<file.GetLength()<<endl;
			//cout << "Last Page" << endl;
			return 0;
		}
		else{
			//cout<<"Number of pages "<<file.GetLength()<<endl;
			currPage+=1;
			file.GetPage(page,currPage);
			ret = page.GetFirst(rec);
			//currPage+=1;
			return 1;
		}

	}

	return 1;	
}

