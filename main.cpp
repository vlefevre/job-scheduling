#include <iostream>
#include <iomanip>
#include <functional>
#include <vector>
#include <math.h>
#include <queue>
#include <time.h>
#include <sys/time.h>
#include <fstream>
#include <string>

#include <omp.h>

#define DEBUG(X) std::cerr << X << "\n";

template<typename T> void print_queue(T& q)
{
	while (!q.empty())
	{
		std::string s = q.top().getName();
		std::cout << s << " ";
		q.pop();
	}
	std::cout << "\n";
}

class Task
{
	public:
		Task(std::string n, double l, bool t, long p, std::function<double(double,long)> s) : name(n), length(l), speedup(s), type(t), np(p), failures(0) {}
		void setName(std::string s) { name = s; }
		std::string getName() const { return name; }
		double time();
		double getPriority() { return priority; }
		void setPriority(double p) { priority = p; }
		bool getType() const { return type; }
		long getProcs() const { return np; }
		void setProcs(long n) { if (type) np=n; }
		int getFailures() const { return failures; }	
		int getMaxFailures() const { return maxFailures; }
		void setMaxFailures(int ff) { maxFailures = ff; }
		void fail() { failures++; }//std::cerr << name << " failed\n"; }
	private:
		std::string name;
		double length;
		bool type;
		long np;
		std::function<double(double,long)> speedup;
		double priority;
		int failures;
		int maxFailures;
};

double Task::time()
{
	if (type) //if moldable
		return speedup(length,np);
	else //if rigid
		return length;
}

struct Result
{
	double exec_time;
	std::vector<Task> tasks;
};

void printResults(Result r)
{
	std::cout << "Execution time: " << r.exec_time << " seconds.\n";
	for (auto t:r.tasks)
	{
		std::cout << "Task " << t.getName() << " failed " << t.getFailures() << " times.\n";
	}
}

void printResultsSHORT(Result r,int p)
{
	std::cout << r.exec_time << " ";
	double tot = 0;
	double max = 0;
	int nb_fail = 0;
	for (auto t:r.tasks)
	{
		double val = t.time()*(t.getFailures()+1);
		tot += val*t.getProcs();
		if (val > max)
			max = val;
		nb_fail += t.getFailures();
	}
	tot /= p;
	if (max > tot)
		std::cout << max;
	else
		std::cout << tot;
	std::cout << " " << nb_fail << "\n";
}

class Event
{
	public:
		Event(double t, bool tp, Task rt) : time(t), related_task(rt), type(tp) {}
		bool type; //false = start, true = end
		double time;
		Task related_task;
};
auto cmpEvent = [](Event left, Event right) { return left.time > right.time; };

void printEvent(Event e)
{
	std::cerr << e.related_task.getName() << " " << e.time << " " << e.type << "\n";
}
void printEventQueue(std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> eventQueue)
{
	return;
	std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> tmpEventQueue(cmpEvent);
	while (!eventQueue.empty())
	{
		Event e = eventQueue.top();
		eventQueue.pop();
		printEvent(e);
		tmpEventQueue.push(e);
	}
	while (!tmpEventQueue.empty())
	{
		Event e = tmpEventQueue.top();
		tmpEventQueue.pop();
		eventQueue.push(e);
	}
	
}

class Simulator
{
	public:
		Simulator(int s, double l, double v) : seed(s), lambda(l), verif_time(v) {}
		void init() { srand(seed); }
		bool fail(Task t);
		double getLambda() const { return lambda; }
		double getVerif() const { return verif_time; }
		void setSeed(double s) { seed=s; }
		void setLambda(double l) { lambda = l; }
		void setVerif(double v) { verif_time = v; }
		std::function<double(Task)> priority_fun;
		bool batch() { return batchVar>0; }
		bool naive() { return naiveVar>0; }
		bool shelves() { return shelfVar>0; }
		void setBatch(int b) { batchVar=b; }
		void setNaive(int b) { naiveVar=b; }
		void setShelves(int b) { shelfVar=b; }
	private:
		double seed;
		double lambda;
		double verif_time;
		int batchVar, naiveVar, shelfVar;
};

bool Simulator::fail(Task t)
{
	return t.getFailures()<t.getMaxFailures();
}

bool checkAvailProc(double startTime, Task t, int startProcs, std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> eQ, Simulator* s)
{

	//std::cerr << "CHECK AVAIL PROC(" << startTime << "," << t.getName() << "," << startProcs << ")\n";

//	auto cmpEvent = [](Event left, Event right) { return left.time > right.time; };
//	std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> tmpEventQueue(cmpEvent);
	if (startProcs < t.getProcs())
		return false;
	if (eQ.empty())
		return true;
	Event e = eQ.top();
//	tmpEventQueue.push(e);
	eQ.pop();
	int currentProcs = startProcs;
	while (e.time < startTime + t.time() + s->getVerif())
	{
		if (e.type)
			currentProcs += e.related_task.getProcs();
		else
			currentProcs -= e.related_task.getProcs();
		if (currentProcs < t.getProcs())
		{
/*				while (!tmpEventQueue.empty())
				{
					eventQueue.push(tmpEventQueue.top());
					tmpEventQueue.pop();
				}*/
				return false;
		}
		if (!eQ.empty())
		{
			e = eQ.top();
			eQ.pop();
		} else
			break;
	}/*
	while (!tmpEventQueue.empty())
	{
		eventQueue.push(tmpEventQueue.top());
		tmpEventQueue.pop();
	}*/

	return true;
}

//batch = true => failed tasks are rescheduled after all tasks are done once
//naive = true => always schedule by decreasing priority (no fill)
//shelves = true => schedule tasks at once and then wait for them to be finished
Result simulate(Simulator& s, std::vector<Task> jobs, long nbProcs, std::string mParam)
{
	double cTime = 0;
	s.init();

	Result r;

	//Init the priority queue of jobs
	for (unsigned i=0; i<jobs.size(); i++)
		jobs[i].setPriority(s.priority_fun(jobs[i]));

	auto cmpTask = [](Task left, Task right) { return left.getPriority() < right.getPriority(); };
	std::priority_queue<Task,std::vector<Task>,decltype(cmpTask)> jobQueue(cmpTask);
	std::priority_queue<Task,std::vector<Task>,decltype(cmpTask)> tmpJobQueue(cmpTask);
	std::priority_queue<Task,std::vector<Task>,decltype(cmpTask)> reservedJobQueue(cmpTask); //to keep the list of reservations made (in order to delete them in case a task fails)
	std::priority_queue<Task,std::vector<Task>,decltype(cmpTask)> failedJobs(cmpTask); //for next batch

	for (Task j:jobs)
		jobQueue.push(j);

	long pAvail = nbProcs;

	//Create a priority queue for the events (ends of tasks)
//	auto cmpEvent = [](Event left, Event right) { return left.time > right.time; };
	std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> eventQueue(cmpEvent);
	std::priority_queue<Event,std::vector<Event>,decltype(cmpEvent)> tmpEventQueue(cmpEvent);

	int m;

	if (mParam == "zero")
		m = 0;
	else if (mParam == "one")
		m = 1;
	else if (mParam == "all")
		m = jobQueue.size();

	begin:
	//Schedule the first tasks

	int cpt = 0;
	while (!jobQueue.empty() && cpt < m)
	{
		Task first = jobQueue.top();
		jobQueue.pop();
		//find next event where to put the task
		
		//std::cerr << "Reserving for " << first.getName(); 

		//if (pAvail >= first.getProcs()) //OK to schedule now as others
		if (checkAvailProc(cTime,first,pAvail,eventQueue,&s)) //check until end of task because of the reservations
		{
			//std::cerr << " at cTime " << cTime << "\n";
			pAvail -= first.getProcs();
			Event e(cTime+first.time()+s.getVerif(),true,first);
			eventQueue.push(e);
			cpt++;
		} else { //or in the future
			int pAvailFuture = pAvail;
			while (!eventQueue.empty())
			{
				Event next = eventQueue.top();
				eventQueue.pop();
				tmpEventQueue.push(next);
				if (next.type)
					pAvailFuture += next.related_task.getProcs();
				else
					pAvailFuture -= next.related_task.getProcs();
				//if (pAvailFuture >= first.getProcs())
				if (checkAvailProc(next.time,first,pAvailFuture,eventQueue,&s))
				{
					//std::cerr << " at time " << next.time << "\n";
					Event e(next.time,false,first);
					eventQueue.push(e);
					Event f(next.time+first.time()+s.getVerif(),true,first);
					eventQueue.push(f);
					reservedJobQueue.push(first);
					cpt++; //number of reservations
					break;
				}
			}
		
			while (!tmpEventQueue.empty())
			{
				eventQueue.push(tmpEventQueue.top());
				tmpEventQueue.pop();
			}
		}
	}

	while (!jobQueue.empty())
	{
		Task first = jobQueue.top();
		jobQueue.pop();
		//if (first.getProcs() <= pAvail) //If enough procs available
		if (checkAvailProc(cTime,first,pAvail,eventQueue,&s))
		{
			//std::cerr << "Task " << first.getName() << " scheduled.\n";
			pAvail -= first.getProcs();
			Event e(cTime+first.time()+s.getVerif(),true,first);
			eventQueue.push(e);
		} else { //otherwise needs to be rescheduled later
			//std::cout << "Task " << first.getName() << " not scheduled.\n";
			tmpJobQueue.push(first);
			if (s.naive())
				break; //When naive is true, we stop as soon as we cannot add the next task with highest priority (like Turek paper)
		}
	}
	//Put the non-scheduled jobs back in the queue
	while (!tmpJobQueue.empty())
	{
		jobQueue.push(tmpJobQueue.top());
		tmpJobQueue.pop();
	}

	//MAIN LOOP
	while (!eventQueue.empty())
	{
		Event e = eventQueue.top();
		eventQueue.pop();
		cTime = e.time;
	
		//std::cout << "At time " << cTime << ":\n";
	
		if (e.type) { //It is an ending event so we need to schedule new tasks


		//std::cout << "Task " << e.related_task.getName() << " (execution #" << e.related_task.getFailures()+1 << ") finished at time " << e.time << "\n";
		if (s.fail(e.related_task)) //If it failed
		{
			//std::cerr << "Task " << e.related_task.getName() << " failed.\nBefore cancellation:\n";
			printEventQueue(eventQueue);
			e.related_task.fail();
			e.related_task.setPriority(s.priority_fun(e.related_task)); //USELESS SO FAR BUT WE MAY WANT TO CHANGE THE PRIORITY OF A FAILED TASK
			while (!reservedJobQueue.empty()) //CANCEL RESERVATIONS
			{
				Task job = reservedJobQueue.top();
				reservedJobQueue.pop();
				int nEvents = 0;
				while (!eventQueue.empty() && nEvents<2)
				{
					Event ee = eventQueue.top();
					eventQueue.pop();
					tmpEventQueue.push(ee);
					/*if (e.related_task.getName() != job.getName()) //discard the event if same name
					else
					{*/
					if (nEvents == 0 && e.type) //the task has already started as we didn't find the begin event
						break;
					nEvents++; //max 2 events for the same task
					if (nEvents == 2) //OK WE FOUND THE RESERVATION (OTHERWISE THE JOB IS RUNNING OR WAS ALREADY DONE) so we can put it back to being scheduled
						jobQueue.push(job); //back to being scheduled
					//}
				}
				while (!tmpEventQueue.empty()) //we put back the events saved
				{
					eventQueue.push(tmpEventQueue.top());
					tmpEventQueue.pop();
				}
			}
			//std::cerr << "After cancellation:\n";
			printEventQueue(eventQueue);
			if (s.batch())
				failedJobs.push(e.related_task);
			else
				jobQueue.push(e.related_task);
		} else {
			//std::cerr << e.related_task.getName() << " finished at time " << cTime << ". Remaining: " << jobQueue.size() << " tasks.\n";
			printEventQueue(eventQueue);
			r.tasks.push_back(e.related_task);
		}
		pAvail += e.related_task.getProcs();

		if (!s.shelves() || eventQueue.empty()) //RESCHEDULE ONLY IF NOT SHELVES OR SHELF FINISHED
		{
			//Schedule the next tasks
			if (mParam == "zero")
				m = 0;
			else if (mParam == "one")
				m = 1;
			else if (mParam == "all")
				m = jobQueue.size();

			int cpt = 0;
			while (!jobQueue.empty() && cpt < m)
			{
				Task first = jobQueue.top();
				jobQueue.pop();
				//std::cerr << "---Reserving for " << first.getName();
				//find next event where to put the task
				

				//if (pAvail >= first.getProcs()) //OK to schedule now as others
				if (checkAvailProc(cTime,first,pAvail,eventQueue,&s)) //check until end of task because of the reservations
				{
					//std::cerr << " at cTime " << cTime << "\n";
					pAvail -= first.getProcs();
					Event e(cTime+first.time()+s.getVerif(),true,first);
					eventQueue.push(e);
					cpt++;
				} else { //or in the future
					int pAvailFuture = pAvail;
					while (!eventQueue.empty())
					{
						Event next = eventQueue.top();
						eventQueue.pop();
						tmpEventQueue.push(next);
						if (next.type)
							pAvailFuture += next.related_task.getProcs();
						else
							pAvailFuture -= next.related_task.getProcs();
						//if (pAvailFuture >= first.getProcs())
						if (checkAvailProc(next.time,first,pAvailFuture,eventQueue,&s))
						{
							//std::cerr << " at time " << next.time << "\n";
							Event e(next.time,false,first);
							eventQueue.push(e);
							Event f(next.time+first.time()+s.getVerif(),true,first);
							eventQueue.push(f);
							reservedJobQueue.push(first);
							cpt++;
							break;
						}
					}
				
					while (!tmpEventQueue.empty())
					{
						eventQueue.push(tmpEventQueue.top());
						tmpEventQueue.pop();
					}
				}
			}

			//Other jobs to schedule (backfilling)
			while (!jobQueue.empty())
			{
				Task first = jobQueue.top();
				jobQueue.pop();
				//if (first.getProcs() <= pAvail) //If enough procs available
				if (checkAvailProc(cTime,first,pAvail,eventQueue,&s)) //check with the existing reservations
				{
					//std::cerr << "---Task " << first.getName() << " scheduled.\n";
					pAvail -= first.getProcs();
					Event e(cTime+first.time()+s.getVerif(),true,first);
					eventQueue.push(e);
				} else { //otherwise needs to be rescheduled later
					tmpJobQueue.push(first);
					if (s.naive())
						break;
				}
			}
			//Put the non-scheduled jobs back in the queue
			while (!tmpJobQueue.empty())
			{
				jobQueue.push(tmpJobQueue.top());
				tmpJobQueue.pop();
			}
		}

		} else { //it is a begin event so we account for the processors used
			pAvail -= e.related_task.getProcs();
		}
		
	}
	if (s.batch() && !failedJobs.empty()) //Batch mode and some jobs need to be re-executed
	{
		while (!failedJobs.empty())
		{
			jobQueue.push(failedJobs.top());
			failedJobs.pop();
		}
		goto begin; //start again with list of failed jobs
	}

	//std::cout << "Execution time: " << cTime << "\n";

	r.exec_time = cTime;

	return r;
}

double pTaskLength(Task t)
{
	return t.time();
}

double pTaskArea(Task t)
{
	return t.time()*t.getProcs();
}

double pTaskProcs(Task t)
{
	return t.getProcs();
}

double pTaskLengthS(Task t)
{
	return -t.time();
}

double pTaskAreaS(Task t)
{
	return -(t.time()*t.getProcs());
}

double pTaskProcsS(Task t)
{
	return -t.getProcs();
}

double pTaskRandom(Task t)
{
	return rand()/(double)RAND_MAX;
}

double linearSpeedup(double seq_length, long nb_proc)
{
	return seq_length/(double)nb_proc;
}

double amdahlSpeedup(double seq_length, long nb_proc)
{
	double seq = 1e-5;
	return seq_length*(seq+(1.0-seq)/(double)nb_proc);
}

int readInput(std::string filename, Simulator* s, std::vector<Task> &v, int* p)
{
	int batch,naive,shelf;
	double lambda,verif,length;
	std::string priority;
	std::string name;
	std::string type;
	std::string speedup;
	int proc;

	double lower_bound = 0; //To evaluate the "optimal" makespan

	std::ifstream input(filename,std::ios::in);	
	input >> lambda >> *p >> verif >> priority >> batch >> naive >> shelf;
	s->setLambda(lambda);
	s->setVerif(verif);
	s->setBatch(batch);
	s->setNaive(naive);
	s->setShelves(shelf);
	if (priority == "length")
		s->priority_fun = pTaskLength;
	else if (priority == "procs")
		s->priority_fun = pTaskProcs;
	else if (priority == "area")
		s->priority_fun = pTaskArea;
	else if (priority == "rlength")
		s->priority_fun = pTaskLengthS;
	else if (priority == "rprocs")
		s->priority_fun = pTaskProcsS;
	else if (priority == "rarea")
		s->priority_fun = pTaskAreaS;
	else if (priority == "rand")
		s->priority_fun = pTaskRandom;
	else {
		std::cerr << "Unrecognized priority function: " << priority << ".\n";
		return -1;
	}

	while (input >> name >> type >> length)
	{
		if (type != "rgd" && type != "mld") {
			std::cerr << "Undefined type of task.\n";
			return -1;
		}
		if (type == "rgd")
		{
			input >> proc;
			v.push_back(Task(name,length,false,proc,linearSpeedup));
			lower_bound += length*proc;
		}
		else
		{
			input >> speedup;
			if (speedup == "lin")
				v.push_back(Task(name,length,true,1000,linearSpeedup));
			else if (speedup == "amd")
				v.push_back(Task(name,length,true,0,amdahlSpeedup));
			else {
				std::cerr << "Unrecognized speedup function.\n";
				return -1;
			}
		}
	}

	//std::cout << "A lower bound on the optimal makespan: " << lower_bound/(*p) << ".\n";

	input.close();

	return 0;
}

int main(int argc, char** argv)
{
	/*Task job1("test",1000,false,100,linearSpeedup);
	Task job2("bigtask",100000,false,1389,linearSpeedup);
	Task job3("shorttask",100,false,382,linearSpeedup);
	Task job4("rand",58443,false,1000,linearSpeedup);
	Task job5("mamamia",29847,false,2000,linearSpeedup);*/
	struct timeval time;
	gettimeofday(&time, NULL);
	double val = (time.tv_sec * 1000) + (time.tv_usec / 1000);
	Simulator s(val, 0,0);
	std::vector<Task> jobs;
	int num_procs = 0;

	int nb_iter = stoi(std::string(argv[3]));
	int ok = readInput(argv[1],&s,jobs,&num_procs);

	std::ifstream failures(argv[2],std::ios::in);

	if (ok==0)
	{
		double avg_time = 0;
		for (int i=0; i<nb_iter; i++)
		{
			std::cerr << i << "\n";
			s.setSeed(val+i);
			int nb_fail_tmp = 0;
			for (unsigned j=0; j<jobs.size(); j++)
			{
				failures >> nb_fail_tmp;
				jobs[j].setMaxFailures(nb_fail_tmp);
			}
			Result r=simulate(s,jobs,num_procs,"zero");
			{
				avg_time += r.exec_time;
			}
			printResultsSHORT(r,num_procs);
		}
		avg_time /= nb_iter;
		//std::cout << avg_time << "\n";
	}

	failures.close();

	return 0;
}
