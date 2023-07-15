
#include "ThreeJoin.h"

using namespace std;

int main(int argc, char* argv[]){
	init_workers();
	SetLCR_Construction(); // 
	worker_finalize();

	return 0;

}
