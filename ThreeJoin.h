#include "../utils/communication.h"
#include "../blogel/BVertex.h"
#include "../blogel/Block.h"
#include "../blogel/BWorker.h"
#include "../blogel/BGlobal.h"
#include "../blogel/BType.h"
#include <queue>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <ext/hash_map>
#include <string.h>
#include <map>
#include <numeric>
#include <utility>
#include <time.h>
#include <deque>
#include <set>
#include <algorithm>
#include <bitset>
#include <vector>
#include <omp.h>

using namespace std;

struct EnumValue {

	vector<int> Edge;

    void set_Edge(int adj_id){
    	Edge.push_back(adj_id);
    }


    friend ibinstream & operator<<(ibinstream & m, const EnumValue & v){
    	m<<v.Edge;

    	return m;
    }


    friend obinstream & operator>>(obinstream & m, EnumValue & v){
    	m>>v.Edge;

    	return m;
    }
};


struct MsgInf {
	int dis;
	int src;
	long long pths;

	MsgInf(){
		dis = -1; src = -1; pths = -1;
	}

    MsgInf(int f, int s){
    	dis = f; src = s; pths = -1;
    }

    void SetValue(int f){
    	dis = f;
    }

    void SetValue2(long long f){
    	pths = f;
    }

    friend ibinstream& operator<<(ibinstream& m, const MsgInf& idm)
    {
    	m << idm.dis;
    	m << idm.src;
    	m << idm.pths;

        return m;
    }

    friend obinstream& operator>>(obinstream& m, MsgInf& idm)
    {
    	m >> idm.dis;
    	m >> idm.src;
    	m >> idm.pths;

        return m;
    }
};


class LCRVertex : public BVertex<VertexID, EnumValue, MsgInf>{
public:
	virtual void compute(MessageContainer& messages){}

	// === 构建 dist ==

	void candidate_source(MessageContainer& messages){
		// 候选点选择，源点激活
		if (global_step_num == 0 and id == src){
			dist[id].first = 0; // 完成发送点的赋值
			newDis.push_back(make_pair(id, 0)); // 便于全局信息的更新

			if (srcmin > 0)
				srcmin = 0;
		}else{ // 接收信息并判断
			for (MessageIter it = messages.begin(); it != messages.end(); it++) {
				int dis = (*it).dis, sid = (*it).src;

				if ( dist[id].first == -1 ){
					dist[id].first = dis;
					newDis.push_back(make_pair(id, dis)); // 便于全局信息的更新

					if (srcmin > dis)
						srcmin = dis;
				}
			}
		}

		if (dist[id].first == global_step_num and 
		    global_step_num < khop){ // 只有和超步相匹配的点才能发送
			
			MsgInf inf(dist[id].first+1, id);

			for (int i=0; i<value().Edge.size(); ++i){
				int vid = value().Edge[i];
				if (dist[vid].first == -1)
					send_message(vid, v2part[vid], inf);
			}
		}

		vote_to_halt();
    }


	void candidate_target(MessageContainer& messages){
		// 候选点选择，源点激活
		if (global_step_num == 0 and id == dst and dist[id].first <= khop){
			dist[id].second = 0; // dist[id].first <= khop 表示两点之间存在路径
			newDis.push_back(make_pair(id, 0)); // 便于全局信息的更新
			if (dstmin > 0)
				dstmin = 0;
		}else{ // 接收信息并判断
			for (MessageIter it = messages.begin(); it != messages.end(); it++) {
				int dis = (*it).dis, sid = (*it).src;

				if ( dist[id].second == -1 and dis + dist[id].first <= khop ){
					dist[id].second = dis;
					newDis.push_back(make_pair(id, dis)); // 便于全局信息的更新
					if (dstmin > dis)
						dstmin = dis;
				}
			}
		}

		if (dist[id].second == global_step_num and global_step_num < khop){ // 只有和超步相匹配的点才能发送
			MsgInf inf(dist[id].second+1, id);

			for (int i=0; i<value().Edge.size(); ++i){
				int vid = value().Edge[i];
				if (dist[vid].second == -1)
					send_message(vid, v2part[vid], inf);
			}
		}

		vote_to_halt();
	}


	// == 构建 VS(以及VS_RESVERSE) ==

	void search_space(MessageContainer& messages){
		MsgInf inf;
		int fflg = 0;

		if (global_step_num == 0 and id == dst){
			inf.SetValue2(1); // 此时只有1条路径
			fflg = 1;
		}else if (messages.size() > 0){ // 没有接收到信息，则不用传递

			for (MessageIter it = messages.begin(); it != messages.end(); it++){
				v2space[v2p[id]][global_step_num] += it->pths;
				if (it->pths > 0 and global_step_num >= 2)
					v2space[v2p[id]][global_step_num] -= v2space[v2p[id]][global_step_num-2];
			}


			inf.SetValue2(v2space[v2p[id]][global_step_num]); // 此时只有1条路径
			fflg = 1;
		}


		if (fflg == 1){
			for (int i=0; i<value().Edge.size(); ++i){
				int vid = value().Edge[i];
				send_message(vid, v2part[vid], inf);
			}
		}

		vote_to_halt();
	}


	// =======边界索引构建策略=========、
	void Bound_Compute(MessageContainer& messages){ // 按道理讲，这个过程不需要接收其他分区顶点的信息

		vote_to_halt();
	}

};


class LCRBlock : public Block<char, LCRVertex, MsgInf> {
public:

	void Boundary_compute(VertexContainer &vertexes){ // 边界图计算,不接收信息

		vote_to_halt();
	}



	virtual void compute(MessageContainer &messages, VertexContainer &vertexes){}

};


class LCRBlockWorker : public BWorker<LCRBlock>{
public:
	virtual LCRVertex *toVertex(char *line){
		LCRVertex *v;
		return v;
	}


	virtual void blockInit(VertexContainer &vertexes, BlockContainer &blocks){
		// === 去重复顶点  ===
		for(int i=0; i<vertexes.size(); ++i){
			vector<int>& edges = vertexes[i]->value().Edge;
			if (edges.size() > 0){
				sort( edges.begin(), edges.end() );	
				int p = 1;
				for( int j = 1; j < (int) edges.size(); ++j ){
					if( edges[j-1] != edges[j] ) 
						edges[p++] = edges[j];
				}
				edges.resize(p);
			}
		}
		
		dist.resize(totalV); // 初始化记录矩阵
		v2p.resize(totalV, -1);
		v2copy.resize(totalV); // 检查是否为复写点

		for (int i=0; i<totalV; ++i){
			dist[i].first  = -1;
			dist[i].second = -1;
		}

		for (int pos=0; pos<v2degree.size(); ++pos){
			sortTotal.push_back(make_pair(v2degree[pos], pos)); // pos就是id
		}

		sort(sortTotal.rbegin(), sortTotal.rend());
		src = sortTotal[k1].second, dst = sortTotal[k2].second;
		
		if (_my_rank == 0)
		cout<<"src: "<<src<<" dst: "<<dst<<endl;
	}


	void AddVertex(char *line){
		// 三个元素  V_A, V_B, Label
		int Vid_A, Vid_B, label, xx;
		char *data = strtok(line,"=");
		char* s1 = strtok(data," ");
		int n=0; Vid_A = atoi(s1);
		while(s1=strtok(NULL," ")){
			n++;
			switch(n){
				case 1: Vid_B = atoi(s1); break;
			}
		}


		int Vpart_A = v2part[Vid_A], Vpart_B = v2part[Vid_B];
		v2degree[Vid_A] += 1;  v2degree[Vid_B] += 1;

		if (Vpart_A != Vpart_B)
			totalLength += 1;

		// 接下来把顶点添加到 vertexes中
		if (Vpart_A == Vpart_B and Vpart_A == _my_rank)
			totalEdges += 1;


		if (Vpart_A==_my_rank){
			if ( vert2place.find(Vid_A) == vert2place.end() ){
				LCRVertex* v = new LCRVertex;
				v->id  = Vid_A; v->bid = 0;
				load_vertex(v);
				vert2place[Vid_A] = vertexes.size()-1;
			}
			vertexes[vert2place[Vid_A]]->value().set_Edge(Vid_B);			
		}

		if (Vpart_B==_my_rank){
			if ( vert2place.find(Vid_B) == vert2place.end() ){
				LCRVertex* v = new LCRVertex;
				v->id  = Vid_B;
				v->bid = 0;
				load_vertex(v);
				vert2place[Vid_B] = vertexes.size()-1;
			}

			vertexes[vert2place[Vid_B]]->value().set_Edge(Vid_A);
		}
	}

	void AddVertexSelf(char *line){
		// 三个元素  V_A, V_B, Label
		int Vid_A, Vid_B;
		char *data = strtok(line,"=");
		char* s1 = strtok(data," ");
		int n=0; Vid_A = atoi(s1);
		while(s1=strtok(NULL," ")){
			n++;
			switch(n){
				case 1: Vid_B = atoi(s1); break;
			}
		}

		totalEdges += 1;

		if (v2part[Vid_A] == _my_rank){
			
			v2degree[Vid_A] += 1;

			if ( vert2place.find(Vid_A) == vert2place.end() ){
				LCRVertex* v = new LCRVertex;
				v->id  = Vid_A; v->bid = 0;
				load_vertex(v);
				vert2place[Vid_A] = vertexes.size()-1;
			}

			if ((Vid_A+Vid_B) % 10 < 11){
				vertexes[vert2place[Vid_A]]->value().set_Edge(Vid_B);
			}
		}


	}

	void all_LCR_vcompute(int flag){
		active_vcount = 0;
		VMessageBufT* mbuf = (VMessageBufT*)get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (BlockIter it = blocks.begin(); it != blocks.end(); it++)
		{
			LCRBlock* block = *it;
			block->activate(); //vertex activates its block
			for (int i = block->begin; i < block->begin + block->size; i++)
			{
				vertexes[i]->activate();
				if (flag == 0)
					vertexes[i]->candidate_source(v_msgbufs[i]);
				else if (flag == 1)
					vertexes[i]->candidate_target(v_msgbufs[i]);
				else
					vertexes[i]->search_space(v_msgbufs[i]);

				v_msgbufs[i].clear(); //clear used msgs
				if (vertexes[i]->is_active())
					active_vcount++;
			}
		}
	}


	void active_LCR_vcompute(int flag){
		active_vcount = 0;
		VMessageBufT* mbuf = (VMessageBufT*)get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (BlockIter it = blocks.begin(); it != blocks.end(); it++)
		{
			LCRBlock* block = *it;
			for (int i = block->begin; i < block->begin + block->size; i++)
			{
				if (v_msgbufs[i].size() == 0)
				{
					if (vertexes[i]->is_active())
					{
						block->activate(); //vertex activates its block
						if (flag == 0)   vertexes[i]->candidate_source(v_msgbufs[i]);
						else             vertexes[i]->candidate_target(v_msgbufs[i]);
						if (vertexes[i]->is_active())
							active_vcount++;
					}
				}
				else
				{
					block->activate(); //vertex activates its block
					if (flag == 0)   vertexes[i]->candidate_source(v_msgbufs[i]);
					else             vertexes[i]->candidate_target(v_msgbufs[i]);
					if (vertexes[i]->is_active())
						active_vcount++;
				}
			}
		}
	}



	void reorder_neighbors(int vid, vector<int>& neighbor){
		vector<pair<int, int> > elems;

		for (int j=0; j<neighbor.size(); ++j){
			if (dist[neighbor[j]].first != -1 and dist[neighbor[j]].second != -1){
				if (dist[vid].first + dist[neighbor[j]].second + 1 <= khop)
					active_Edges += 1, elems.push_back(make_pair(dist[neighbor[j]].second, neighbor[j]));
			}
		}

		sort(elems.begin(), elems.end());
		vector<int>().swap(neighbor);

		for(int k=0; k<elems.size(); ++k)
			neighbor.push_back(elems[k].second);
	}


	void VertexDivide(vector<pair<long long, vector<int>> >& vPath,
			          vector<long long>& path1s, 
					  vector<vector<int>>& srcs){
		
		long long searchs = 0;
		vector<int> cntts(_num_workers);
		for (int i=0; i<vPath.size(); ++i){
			int minP = min_element(path1s.begin(),path1s.end()) - path1s.begin();
			// int minP = v2part[vPath[i].second[0]];
			path1s[minP] += vPath[i].first;

			cntts[minP] += 1;
			if (minP == _my_rank)
				srcs.push_back(vPath[i].second), searchs += vPath[i].first;
		}

		cout<<"rank: "<<_my_rank<<"   space: "<<searchs<<endl;
	}


	void print_neighbors(vector<int>& neighbor){
		vector<pair<int, int> > elems;

		for (int j=0; j<neighbor.size(); ++j){
			cout<<neighbor[j]<<"  ";
		}
		cout<<endl;
	}


	void DFS_Middle(int u, int hop, vector<int>& flag_, 
					vector<int>& s_, vector<vector<int> >& subs){
		// 基于MiddlePaths完成路径拼接，subs中存储的都是2-hop的路径
		s_.push_back(u); 
		flag_[u] = 1;

		if ( dist[u].second == 1 and s_.size() > 1 ){
			for (int ii=0; ii<subs.size(); ++ii){
				int vvid = subs[ii][0];
				if (flag_[vvid] == 0){ // 跟中间点没有冲突
					paths += 1;
				}
			}
		}

		vector<int>& Local = graphs[v2p[u]];
		
		for (int i=0; i<(int)Local.size(); ++i){

			int v = Local[i];
			
			if (s_.size() + dist[v].second > hop ) break;

			if (v == dst){

			}else if(flag_[v] == 0 and s_.size() == hop-1){
				flag_[v] = 1;
				for (int ii=0; ii<subs.size(); ++ii){
					int vvid = subs[ii][0];
					if (flag_[vvid] == 0){ // 跟中间点没有冲突
						paths += 1;
					}
				}
				flag_[v] = 0;
			}else if (flag_[v] == 0){
				DFS_Middle(v, hop, flag_, s_, subs);
			}
		}

		s_.pop_back();
		flag_[u] = 0;

		return;
	}

	


	void GraphRestruct(){
		graphs.resize(active_nums);
		Locals.resize(_num_workers);

		for (int i=0; i<vertexes.size(); ++i){
			int id = vertexes[i]->id;

			if (v2p[id] == -1) continue;

			vector<int>& edge = vertexes[i]->value().Edge;
			edge.push_back(id);

			if (id!= src and id!=dst ){
				// src端的任务已经完成了，所以可以全局复写图了 v2copy[id] == 1 or v2copy[id] == 2 or v2copy[id] == 3
				for (int k=0; k<_num_workers; ++k)
					Locals[k].push_back(edge);
			}
		}

		all_to_all(Locals);
		worker_barrier();

		for (int i=0; i<Locals.size(); ++i){
			for (int j = 0; j < Locals[i].size(); ++j){
				vector<int>& adjList = Locals[i][j];
				int place = v2p[adjList[adjList.size()-1]];

				adjList.pop_back();

				graphs[place].insert(graphs[place].end(),
										adjList.begin(),
										adjList.end());
			}
		}
	}


	void PathCollect(){
		vector<int> pth(2);

		for(int i=0; i<totalV; ++i){	
			if (dist[i].first == 1){
				if (i == dst){
					paths += 1; // 1-hop path
				}else if (_my_rank == v2part[i]){
					pth[0] = i;
					for (int kk=0; kk<vertexes[vert2place[i]]->value().Edge.size(); ++kk){
						int vid = vertexes[vert2place[i]]->value().Edge[kk];

						if (v2p[vid] == -1 or vid == src) continue;
						
						if (vid == dst){
							paths += 1; // 2-hop path
						}else{
							pth[1] = vid; // pth 存储了 src-1-2 这一条路径
							SubP.push_back(pth);
							srcVertex.push_back(vid); // 所有的2-hop点，可能重复
						}
					}
				}
			}
		}

		// === 确定src的所有2-hop邻居 ===
		vector< vector<int> > SrcV(_num_workers);
		vector< vector<vector<int> > > SubPaths(_num_workers);
			
		for(int i=0; i<_num_workers; ++i){
			SubPaths[i] = SubP;
			SrcV[i] = srcVertex;
		}

		all_to_all_cat(SrcV, SubPaths);
		worker_barrier();

		// == 收集目标点 ==
		set<int> vect;
		for(int i=0; i<_num_workers; ++i)
			vect.insert(SrcV[i].begin(), SrcV[i].end());

		srcVertex.clear();

		for(auto it=vect.begin(); it!=vect.end(); ++it){
			v2copy[*it] = 1;
			srcVertex.push_back(*it);
		}

		// == 收集 子路径 == SubPaths 包含了所有的2-hop路径
		SubP.clear();
		for(int i=0; i<_num_workers; ++i){
			vector<vector<int> >& pthss = SubPaths[i];
			for (int j=0; j<pthss.size(); ++j){
				int last_id = pthss[j][1];
				MiddlePaths[last_id].push_back(pthss[j]);
				if (dist[last_id].second == 1 and _my_rank == v2part[last_id] and khop >= 3)
					paths += 1;
			}
		}

		vector< vector<vector<int> > >().swap(SubPaths);
		// vector<int>().swap(srcVertex);
	}


	void PathPredict(){
		v2space.resize(active_nums, vector<long long>(khop+1)); 
		for (int hop=1; hop<=khop-kk1; ++hop){
			for (int vnod=0; vnod<totalV; ++vnod){
				if (v2p[vnod] == -1) 
					continue;
				
				if (hop == 1){
					if (dist[vnod].second == 1)
						v2space[v2p[vnod]][1] = 1;
				}else{
					vector<int>& edge = graphs[v2p[vnod]];
					for (int j=0; j<edge.size(); ++j){
						if (edge[j] == src or edge[j] == dst)
							continue;

						v2space[v2p[vnod]][hop] += (v2space[v2p[edge[j]]][hop-1]);
					}
				}
			}
		}
	}


	void SubtaskProduce(int vid, vector<int>& idL, vector<int>& ff_, int d){
		idL.push_back(vid);
		ff_[vid] = 1;

		for (int i=0; i<graphs[v2p[vid]].size(); ++i){
			int uu = graphs[v2p[vid]][i];

			if (ff_[uu] == 1) continue;

			// if (idL.size()+2+dist[uu].second > khop ) break;
			
			long long spp = 0;
			for (int kk=1; kk<khop-d; kk++)
				spp += v2space[v2p[uu]][kk];

			spp = spp * MiddlePaths[idL[0]].size();

			if (spp < Wavg){
				idL.push_back(uu);
				pair<long long, vector<int> > elem(spp, idL);
				Tasks1.push_back(elem);
				idL.pop_back();
			}else
				SubtaskProduce(uu, idL, ff_, d+1);
			
		}

		idL.pop_back();
		ff_[vid] = 0;

		return;
	}


	void TaskDivision(){
		
		for (int i=0; i<srcVertex.size(); ++i){
			for (int j=0; j<=khop-2; ++j){
				long cnt11 = MiddlePaths[srcVertex[i]].size();
				Wavg += v2space[v2p[srcVertex[i]]][j]*cnt11;
			}
		}
		Wavg = Wavg / _num_workers;

		for (int i=0; i<srcVertex.size(); ++i){
			int active_id = srcVertex[i], cnt22 = MiddlePaths[active_id].size();
			
			long long searchbound  = 0;
			for (int sss=0; sss<=khop-2; sss++)
				searchbound += v2space[v2p[active_id]][sss];
			searchbound = searchbound * cnt22;

			vector<int> ee1;
			
			if (searchbound > Wavg){
				vector<int> ff_(totalV);
				ff_[src] = 1, ff_[dst] = 1;

				SubtaskProduce(active_id, ee1, ff_, 2);
				// for(int kk=0; kk<graphs[v2p[active_id]].size(); ++kk){
				// 	int vvid = graphs[v2p[active_id]][kk];
				// 	ee1.push_back(vvid);
				// 	pair<long long, vector<int> > elem(v2space[v2p[vvid]][khop-kk1-1], ee1);
				// 	Tasks1.push_back(elem);
				// 	ee1.pop_back();
				// }
			}else{
				ee1.push_back(active_id);
				pair<long long, vector<int> > elem(searchbound, ee1);
				Tasks1.push_back(elem);
			}
		}

		sort(Tasks1.rbegin(), Tasks1.rend());
		active_tasks = Tasks1.size();

		srcVertex.clear();
		vector<long long> paths1(_num_workers);

		VertexDivide(Tasks1, paths1, srcVList);
		vector<pair<long long, vector<int> > >().swap(Tasks1);
	}


	void run_LCR(const WorkerParams& params){
		
		ifstream infile, infile1, infile2; // 先把分区文件读取
		
		string s;
		int nnn = 100000000, ii=0;
		v2degree.resize(nnn);
		while(ii<nnn){
			v2part.push_back( ii % _num_workers ); // 每个分区记录所有顶点的分区    nnn % _num_workers  atoi(part)
			ii += 1;
		}

		// const char *part_path = params.partition_path.c_str(); 
		// infile.open(part_path);
		// string s;
		// if(!infile.is_open()){
		// 	cout<<"No such file!"<<endl;
		// 	exit(-1);
		// }

		// int nnn = 0;
		// while(getline(infile, s)){
		// 	char* part = new char[strlen(s.c_str())+1];
		// 	strcpy(part, s.c_str());
		// 	v2part.push_back( atoi(part) );
		// 	v2degree.push_back(0);
		// 	delete part;
		// 	nnn += 1;
		// }


		k1 = params.src, k2 = params.dst;
		khop = params.khop;
		kk1 = 2, kk2 = 1;

		int ff1 = _my_rank % 10;

		string ff = params.input_path + to_String(ff1);
		const char *filepath; //   
		
		if (_num_workers >= 10)
			filepath = ff.c_str();
		else
		    filepath  = params.input_path.c_str(); //"data//Amazon_New.txt";


		// ======================
		infile1.open(filepath);
		if(!infile1.is_open()){
			cout<<"No such file!"<<endl;
			exit(-1);
		}

		while(getline(infile1, s)){
			char* strc = new char[strlen(s.c_str())+1];
			strcpy(strc, s.c_str());
			if (_num_workers < 10)
				AddVertex(strc);
			else
				AddVertexSelf(strc);
			delete strc;
		}

		worker_barrier();

		totalV = all_sum(vertexes.size());
		long long totalE = all_sum_LL(totalEdges);
		v2part.resize(totalV);
		v2degree.resize(totalV);



		// == 基于sub的v2degree值是不完整的 ==
		if (_num_workers >= 10){
			vector<vector<int> > vD2(_num_workers);
			for (int i=0; i<_num_workers; ++i){
				if (i == _my_rank) 
					continue;
				vD2[i] = v2degree;
			}

			all_to_all(vD2);
			worker_barrier();

			for (int i=0; i<_num_workers; ++i){
				for (int j=0; j<vD2[i].size(); ++j){
					v2degree[j] += vD2[i][j]; 
				}
			}
		}

		if (_my_rank == 0){
			cout<<"Loading Finish!!"<<endl;
			cout<<"total Vertices: "<<totalV<<"  "<<totalE/2<<endl;
			cout<<"Cutting Edges: "<<totalLength<<endl;
		}

		// 这里的特殊情况是 每个worker上，只有一个block。
		int prev = -1, pos, xxx = 0;
		LCRBlock* block = NULL;
		for (pos = 0; pos < vertexes.size(); pos++){
			int bid = vertexes[pos]->bid; // 最开始构建顶点时，就已经确定了的 block id
			if (bid != prev){
				if (block != NULL){
					block->size = pos - block->begin;
					blocks.push_back(block);
				}
				block = new LCRBlock;
				prev = block->bid = bid;
				block->begin = pos;
			}
		}

		if (block != NULL){
			block->size = pos - block->begin;
			blocks.push_back(block);
		}
		active_bcount = getBNum(); //initially, all blocks are active


		get_bnum() = all_sum(getBNum());
		get_vnum() = all_sum(getVNum());

		blockInit(vertexes, blocks); //setting user-defined block fields, 用户可以指定给block构建什么信息
		
		vmessage_buffer->init(vertexes);
		bmessage_buffer->init(blocks);

		worker_barrier();
       long long step_vmsg_num;
        long long step_bmsg_num;
        long long global_vmsg_num = 0;
        long long global_bmsg_num = 0;
// =================================================================
if (compute_mode == VB_COMP){
	
	float t = omp_get_wtime();
	// ======== 第一部分：构建dist，并排序 =======
	global_step_num = 0;
	int global_flag = 0;

	while (true){

		if (global_flag > 1) break;

		while (true){

			char bits_bor = all_bor(global_bor_bitmap);
			if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
				break;

			int wakeAll;
			if (global_step_num == 0)   wakeAll = 1;
			else            wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);

			if (wakeAll == 0){
				active_vnum() = all_sum(active_vcount);
				active_bnum() = all_sum(active_bcount);
				if (active_vnum() == 0 && active_bnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
					break; //all_halt AND no_msg
			}
			else{
				active_vnum() = get_vnum();
				active_bnum() = get_bnum();
			}
			//===================
			clearBits();

			all_LCR_vcompute(global_flag);

			step_vmsg_num = master_sum_LL(vmessage_buffer->get_total_msg());
			step_bmsg_num = master_sum_LL(bmessage_buffer->get_total_msg());
			if (_my_rank == 0) {
				global_vmsg_num += step_vmsg_num;
				global_bmsg_num += step_bmsg_num;
			}

			vmessage_buffer->sync_messages();

			worker_barrier();

			global_step_num++;

			// ===== 加一个全局信息更新的操作  =====
			infVector.resize(_num_workers);
			for (int i=0; i<_num_workers; ++i){
				if (i != _my_rank){
					infVector[i].resize(1);
					infVector[i] = newDis;
				}
			}
			all_to_all(infVector);
			worker_barrier();

			for (int i=0; i<_num_workers; ++i){

				if (i == _my_rank) continue;

				for (int j=0; j<infVector[i].size(); ++j){
					pair<int, int>& elem = infVector[i][j];
					if (global_flag == 0)
						dist[elem.first].first  =  elem.second; 	// 更新信息
					else
						dist[elem.first].second =  elem.second; 	// 更新信息
				}
			}

			vector<vector<pair<int, int> > >().swap(infVector);
			vector<pair<int, int> >().swap(newDis);

			if (global_step_num > khop)
				break;
		}

		global_flag += 1;
		global_step_num = 0;
	}

	// == 邻居点重排序  ===
	int cnts = 0;
	
	for (int i=0; i<totalV; ++i){
		if(dist[i].first == -1 or dist[i].second == -1) 
			continue;

		v2p[i] = active_nums;
		active_nums += 1;

		if (dist[i].second == k2){
			v2copy[i] = 2; 
		}else if(dist[i].first > kk1 and i!=dst){
			v2copy[i] = 3; // 复写至num-1个分区
		}else { // 包括src，src-1，dst 
			v2copy[i] = 4;
		}

		if (v2part[i] == _my_rank){
			reorder_neighbors(i, vertexes[vert2place[i]]->value().Edge);
		}
	}

	
	// ===  收集路径信息  ===
	PathCollect();


	// ==== 执行图的重构 ====
	GraphRestruct();

	// ==== 预测路径数量  ====
	PathPredict();

	// ==== 完成srcVertex中任务的划分 ====
	TaskDivision();

	// // // === 各节点并行枚举路径  ===
	kmax = khop - kk1;

	vector<int> flag1, s1;
	flag1.resize(totalV);
	flag1[src] = 1, flag1[dst] = 1;
	for (int i=0; i<srcVList.size(); ++i){
		vector<int>& vL = srcVList[i];
		int vid = vL[vL.size()-1], v2 = vL[0];

		for (int ii=0; ii<vL.size()-1; ++ii){
			flag1[vL[ii]] = 1;
		}

		vL.pop_back();

		DFS_Middle(vid, kmax, flag1, vL, MiddlePaths[v2]);

		for (int ii=0; ii<vL.size(); ++ii){
			flag1[vL[ii]] = 0;
		}

	} 

	worker_barrier();

	long long totalPaths = all_sum_LL(paths);


	long edgess = all_sum(active_Edges);
	float ttt = omp_get_wtime()-t;
	if (_my_rank == 0){ // 只需要存储边界图的所有边
		string Bound_filename = "ThreeJ_WB_"+to_string(_num_workers);
		const char *file = Bound_filename.c_str();
		fstream outfileX;
		outfileX.open(file, ios::out);


		
		long long comm = float(global_vmsg_num)/(1024*1024*8);
		long long comm2 = float(edgess/2*_num_workers+active_nums*_num_workers+active_tasks)*4/(1024*1024*8);

		cout<<"Paths: "<<totalPaths<<"  "<<comm<<"  "<<comm+comm2<<endl;
		cout<<"time: "<<ttt<<endl;
		outfileX<<"Paths: "<<totalPaths<<"  "<<comm+comm2<<endl;
		outfileX<<"time: "<<ttt<<endl;
	}

}
	}
};



void SetLCR_Construction(string in_path, string partition_path, string out_path, int src, int dst, int khop){
	WorkerParams param;
	param.input_path=in_path;
	param.partition_path = partition_path;
	param.output_path=out_path;
	param.src = src;
	param.dst = dst;
	param.khop = khop;
	param.force_write=true;
	param.native_dispatcher=false;
	LCRBlockWorker worker;
	worker.set_compute_mode(LCRBlockWorker::VB_COMP);

	worker.run_LCR(param);
};














