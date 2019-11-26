#include <iostream>
#include <vector>
#include <string>
#include <queue>
#include <random>
#include <math.h>
#include <list>
#include <memory>
#include <chrono>
#include <unistd.h>
#include <cstdlib>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <algorithm>


using namespace std;
using namespace std::chrono;
default_random_engine generator;


//Job replicator 5x faster than python!
class Job   
{
public:
    double size   = 0;
    double   t0   = -1;
    double   t1   = -1;
    double   t2   = -1;
    double   wt   = -1;
    double   st   = -1;
    bool     ex   = true;
    bool   repl   = true;

    Job(double time){
        size = time;
    }

    Job(){}
};


/*SERVER*/
class Server
{

public:
    int    id   = 0; //ID  
    double rate = 0; //Server rate
    double ct   = 0; //Current time
    queue<shared_ptr<Job>> b; //Job queue
    double queue_length = 0; //Sum of job sizes currently in queue
    double idletime = 0; //Time spent idel
    int n_jobs = 0; //number of jobs in system      
    double tau = 0; //Deadline
    double  B = 0; //Time spent working
    int     N = 0; //Jobs complteted
    double WT = 0; //Total waiting time
    double ST = 0; //Total sojourn  time
    double ST2= 0; //Total sojorun time second power
    int C     = 0; //Nr of jobs that exceed deadline 
    int CR    = 0; //Nr of replicated jobs exceed deadline
    int CN    = 0; //Nr of non-repl jobs that exceed deadl
    double t_idle = 0; //zero when idle queue!
    double r_costs = 0;

    //Variables for moment matching    
    double  phi = 0;
    double  beta = 0;

    std::string dist; //Distribution
    double D = 0; //Weibull beta param     

    //ID, rate, probability distrobution, k for weibull
    Server(int x, double r, string d = "e", double k = 1)
    {
        id   = x;
        rate = r;
        dist = d;
        D = k;
    }   

    //Rate of server depending on distribution   
    double get_rate() {
        double r;
        if(dist=="e") {
            exponential_distribution<double> rng(rate);
            r = rng(generator);
        }
        else if(dist=="w") {
            weibull_distribution<double> rng(D, rate);
            r = rng(generator);

        }
        else if(dist=="d") {
            uniform_real_distribution<double> rng(0, 1);
            double x = rng(generator);
            if(x < (sqrt(2)+2)/4 ) {
                    r = (2-sqrt(2));
                }
                else{
                    r =(sqrt(2)+2);
                }
        }
        
        else if(dist=="f") {
            r = 1;
        }

        else if(dist=="u") {
            uniform_real_distribution<double> rng(0, 2*rate);
            r = rng(generator);
        }

        // A invalid distribution was selecd so exp dist is used
        else {
            exponential_distribution<double> rng(rate);
            r = rng(generator);
        }

        
        return r;
    }


    //Look at next job in queue
    shared_ptr<Job> j_next()
    {
        return b.front();
    }

    //Server works a job
    void process_one()
    {   

        shared_ptr<Job> job = j_next(); 
        b.pop();
        n_jobs -=1;
        queue_length -= job->size/rate;
        if (n_jobs==0) {
            t_idle = 0;
        }

    }
    Server(){}

    void skip_one() {
        shared_ptr<Job> job = j_next();
        queue_length -= job->size/rate;
        b.pop();
        n_jobs = n_jobs-1;
    }

    double idle(double t) {
        return max(t,t_idle);
    }


    void add(shared_ptr<Job> job,double at){
        
        double speed = job->size/rate;
        job->t0 = at;
        job->t1 = idle(at);
        queue_length += speed;
        if(beta>0) speed = speed + phi - beta; //Moment matching
        job->wt = job->t1-job->t0;
        job->st = speed + job->wt;
        job->t2 = job->t1 + speed;
        b.push(job);
        n_jobs += 1;
        t_idle = job->t2;
        ct  += speed;
        B   += speed;
        N   += 1;
        WT  += job->wt;
        ST  += job->st;
        ST2 += pow(job->st,2);
        if(job->st>tau){
            C += 1;
            if (job->repl) {
                CR += 1;
            }
            else {
                CN += 1;
            }
        }
    }

};

/**************POLICIES*********/

class Policy
{
public:
    string id = "";
    default_random_engine gener;
    int prev_choice =2;
    int next_choice =-1;
    vector<int> SITAlist;
    Policy(string idx) {
        id = idx;
    }

    int action(vector<Server>& servers, double at, shared_ptr<Job> job) {
        //Job created
        
        
        int j = -1;

        //Job replication
        if(id == "JR") {
            cout<<":("<<endl;
        }

        //Random discrete
        else if(id == "RND") {
            double range = 0;
            for(Server& s: servers) {
                range += s.rate;
            }

            uniform_real_distribution<double> rng(0.0,range);
            double r = rng(gener);
            double srates = 0;

            for(Server& s: servers){
                
                if(srates<=r) {
                    j = s.id;

                }

                srates+= s.rate;
            }

            //j= r;
    
        }

        else if(id == "RR") {
            ++next_choice;
            if(next_choice>=servers.size() or next_choice<0) next_choice =0;
            j = next_choice ;

        }

        else if(id == "RRe") {
            if(prev_choice==2){
                next_choice = 1;
                prev_choice = 0;
                j = 0;
            }
            else if (prev_choice==1) {
                next_choice = 2;
                prev_choice = 0;
                j = 0;
            }
            else if (prev_choice==0) {
                j = next_choice;
                prev_choice = next_choice;

            }

        }

        else if (id == "JSQ") {

            int n = -1;
            int k = 0;
            for(Server& s: servers) {
                if (n<0 or s.n_jobs<n) {
                    n = s.n_jobs;
                    j = s.id;
                    k = 1;                    
                }
                /*
                
                else if(s.n_jobs==n) {
                    k++;
                    uniform_int_distribution<int> rng(1, k);
                    if(rng(generator)==1) {
                        j = s.id;
                    }

                }
                */
                
            }
            
        }


        else if (id == "SED") {

            double n = -1;
            int k = 0;
            for(Server& s: servers) {
                if (n<0 or (s.n_jobs+1)*1.0/s.rate<n) {
                    n = (s.n_jobs+1)*1.0/s.rate;
                    j = s.id;
                    k = 1;                    
                }
                /*
                else if(s.n_jobs==n) {
                    k++;
                    uniform_int_distribution<int> rng(1, k);
                    if(rng(generator)==1) {
                        j = s.id;
                    }
                }
                  */  
            }
            
        }

        else if (id == "LWL") {
            double t = -1;
            int k =0;
            for(Server& s: servers){
                double ti = s.idle(at);
                if( t==-1 or ti<t) {
                    j=s.id;
                    t = ti;
                    k = 1;
                }
                else if(t==s.queue_length) {
                    k++;
                    uniform_int_distribution<int> rng(1, k);
                    if(rng(generator)==1) {
                        j = s.id;
                    }
                }

            }
            

        }
        //Hacky way of doing sita, need to manually add intervals, improve later.
        else if (id == "SITA") {
            if(SITAlist.size()<1) {
                vector<int> ivec(servers.size());
                std::iota(ivec.begin() ,ivec.end(), 0);
                std::shuffle(ivec.begin(), ivec.end(), generator);
                

                for (int i=0; i<ivec.size(); i++) 
                        SITAlist.push_back(ivec[i]); 
            }
            //vector<double> eps = {0,0.662433, 1.0584,1.45962, 1.9183, 2.50773,3.43557}; // 7 servers
            //vector<double> eps = {0,0.96127, 1.678346,2.69263}; // 4 servers
            vector<double> eps = {0,1.678346}; // 2 servers
            for(int i =0; i<servers.size(); i++) {
                if(job->size >(eps[i])) j = SITAlist[i];
            }

    
        }
        return j;
        //else if (id = SED)


    }
};



/*SIMULATOR*/
class Simul
{

public:
    vector<Server> servers; 
    double c = 0; //Server capacity
    int n = 0; //Number of servers
    double lambda = 0; //Arrival rate
    vector<double> lalist = {}; //Each servers arrival rate in W-model
    double t  = 0; //wall-clock
    double t0 = 0; //Start of simulation
    double t1 = 0; //End of simulation
    double w_N =1; 
    double w_B =0;
    exponential_distribution<double> arrival; //Arrival generator
    string process = "exp"; //Arrival processs

    //Stats
    double Nsum = 0;
    double Bsum = 0;
    double Rsum = 0;
    double Costs= 0;
    //Initilazie simulation with server rates and type of distribution
    Simul(list<double>& serv, string dist, double k, string arr) {
        int i = 0;
        process = arr;
        for(double x : serv) {
            Server s = Server(i++, x, dist, k);
            servers.push_back(s);
            c =+ x;
        }
        n = serv.size();
    }

    std::vector<int> state() {
        std::vector<int> nrjobs;
        for(Server& s: servers) {
            nrjobs.push_back(s.n_jobs);
        }
        return nrjobs;

    }



    double meanN() {
        return Nsum / (t-t0);
    }
    double meanB() {
        return Bsum/(t-t0);
    }
    double meanCost() {
        return Costs /(t-t0);
    }
    
    int n_jobs(){
        int nr_of_jobs=0;
        for(Server& s: servers){
            nr_of_jobs += s.n_jobs;
        }
        return nr_of_jobs;
    }

    double r_costs() {
        double r_costs=0;
        for(Server& s: servers){
            if(s.r_costs>0) r_costs += s.r_costs;
        }
        return r_costs;
    }

    double n_busy(){
        int busy = 0;
        for(Server& s: servers){
            if(s.r_costs>0) busy++;
        }
    }

    void pass_time(double t2){
        double dt=t2-t;
        t=t2;
        Nsum += dt* n_jobs();
        Bsum += dt*n_busy();
        Rsum += dt*r_costs();
        Costs = Nsum + Rsum;
    }

    int next_job(double at) {
        int next_server = -1;
        for (Server& s : servers) {
            if(s.n_jobs>0 and s.b.front()->t2<at) {
                next_server = s.id;
                at = s.b.front()->t2;
            }
        }
        return next_server;
    }

    //Work the jobs
    void process_jobs(double at){
        
        int s = next_job(at);
        //int temp = at;
        
        int i =0;
        while (s != -1) {
            Server& serv = servers.at(s);
            shared_ptr<Job> job = serv.j_next();
            pass_time(job->t2);

            if(job->ex == true){

                serv.process_one();
                job->ex = false;
            }

            else{
                serv.skip_one();
            }

            s = next_job(at);

        }

    }

    double get_arrival(double la) {
        double at = 0;
        if (process == "exp") {
            arrival = exponential_distribution<double>(la);
            at = arrival(generator);    
        }

        else if (process == "fix") {
            at = 1/la;
        }
        return at;
    }

    //Start simulation
    void simulate(double ti, double la, double tau, vector<string> pollist, int seed){

        generator.seed(seed);
        for(Server& s: servers) {
            s.tau = tau;
        }
        t1= ti;
        double at = get_arrival(la);
        Policy pol = Policy(pollist[0]);
        while(at<ti) {
            process_jobs(at);
            
            exponential_distribution<double> rng(1);
            double r = rng(generator);
            Job newjob = Job(r);
            auto sp = make_shared<Job>(newjob);
            int j = pol.action(servers, at, sp);
            servers[j].add(sp,at);
            at += get_arrival(la);
        }
        
        process_jobs(t1);
    }
    //start simulation with multiple dispatchers.
    //TODO Combine so only one simulate function required.
    void simMD(double ti, vector<double> lalist, double tau,  vector<string> pollist,double seed=2) {
        generator.seed(seed);

        vector<Policy> pols; 

        for(string p: pollist) {
            pols.push_back(Policy(p));

        }
        
        for(Server& s: servers) {
            s.tau = tau;
        }


      
        vector<double> ati;
        for(int k = 0; k<lalist.size(); k++){
            ati.push_back(get_arrival(lalist[k]));

        }

        double first = *std::min_element(ati.begin(), ati.end());
        int j = std::min_element(ati.begin(), ati.end()) - ati.begin();

        while(*std::min_element(ati.begin(), ati.end())<ti) {
            j = std::min_element(ati.begin(), ati.end()) - ati.begin();
            process_jobs(ati[j]);

            exponential_distribution<double> rng(1);
            double r = rng(generator);
            Job newjob = Job(r);
            auto sp = make_shared<Job>(newjob);
         
            

            int i = pols[j].action(servers, ati[j], sp);

            servers[i].add(sp,ati[j]);
            ati[j] += get_arrival(lalist[j]);

        }
        process_jobs(ti);
    }


    //Moment matching weight
    vector<double> beta() {
        vector<double> b;
        for(Server& s: servers) {
            b.push_back(s.B/s.N);
        }
        return b;
    }

    //PERFORMANCE STATS

    double meanST() {
        int N = 0;
        long double T = 0;
        for(Server& s: servers) {
            N += s.N;
            T += s.ST;
        }
        return T/max(1,N);
    }

    double meanWT() {
        int N = 0;
        double W = 0;
        for(Server& s: servers) {
            N += s.N;
            W += s.WT;
        }
        return W/max(1,N);
    }

    int stat_jobs() {
        int N = 0;
        for(Server& s: servers) {
            N += s.N;
        }
        return N;
    }

    double varST() {
        int N = 0;
        double T = 0;
        for(Server& s: servers) {
            N += s.N;
            T += s.ST2;
        }
        return (T/max(1,N)-pow(meanST(),2));
    }


    //Deadline
    double meanC() {
        int N = 0;
        double C = 0;
        for(Server& s: servers) {
            N += s.N;
            C += s.C;
        }
        return (C/N);
    }
    /*
    double meanB() {
        int N = 0;
        double B = 0;
        for(Server& s: servers) {
            N += s.N;
            B += s.B;
        }
        return B/N;
    }
    */
};


void print_numeric(Simul &sim) {
    double jobs = sim.stat_jobs();
    double wt = sim.meanWT();
    double st = sim.meanST();
    double var = sim.varST();
    double C = sim.meanC();

    printf("%0.f %.12f %.12f %.12f %.12f ", jobs, wt, st, var, C);

}

void print_result(Simul &sim) {
    cout << "Jobs   : " << sim.stat_jobs() << endl;
    cout << "meanWT : " << sim.meanWT() << endl;
    cout << "meanST : " << sim.meanST() << endl;
    cout << "varST  : " << sim.varST() << endl;
    cout << "meanC  : " << sim.meanC() << endl;
    cout << "meanB  : " << sim.meanB() << endl;
}

//Prints if no options selected not complete 
void help() {
    cout << "Options: [-n server rates] [-l arrival rate] [-t sim time] [-s seed] ";
    cout << "[-d deadline] [-r 0/1 print numeric] [-c 1/2/3 (normal/md-model/w-model)]";
    cout << "[-p Distribution (e/f/w/u/d)] [-w W-model arrival rates]" << endl;
}             



/* MAIN */

int main(int argc, char *argv[]) {
    if (argc == 1) {
        help();
        exit(1);
    }

    //Initialize defaults
    double la = 0.8;
    int c = 0; int lcount = 0; int index = 0; int s = 1;
    int time = 10000; char* next; double d = 2; 
    std::vector<char*> login; bool nr = false;
    int type = 1; std::string dist = "e"; double k = 1;
    vector<double> lalist = {0.5,0.5};
    vector<string> pollist = {"JR", "JSQ"};
    std::string arr = "exp";


    //Options available
    while ((c = getopt (argc, argv, "n:l:t:d:r:s:c:p:k:w:x:a:")) != -1) {
        switch(c) {
            case 'n': //Server rates list
                index = optind-1;
                while(index<argc) {
                    next = strdup(argv[index]);
                    index++;
                    if(next[0] != '-') {
                        login.push_back(next);
                    }
                    else break;
                }
                optind = index - 1;
                break;

            case 'l': //Arrival rate
                if(optarg) la = atof(optarg);
                break;
            case 't': //Sim time
                if(optarg) time = atof(optarg);
                break;
            case 'd': //Deadline
                if(optarg) d = atof(optarg);
                break; 
            case 'r': //Print numerical
                if(atof(optarg) == 1) nr = true;
                break;
            case 's': //Seed
                if(optarg) s = atof(optarg);
                break;
            case 'c': //Case (norma/Wmodel/moment matching)
                if(optarg) type = atof(optarg);
                break;
            case 'p': //Probability distribution
                if(optarg) dist = optarg;
                break;
            case 'k': //Beta for weibull distr
                if(optarg) k = atof(optarg);
                break;
            case 'w': //W-model arrivals list, available in other version
                index = optind-1;
                lalist.clear();
                while(index<argc) {
                    next = strdup(argv[index]);
                    index++;
                    if(next[0] != '-') {
                        lalist.push_back(atof(next));
                    }
                    else break;
                }
                optind = index - 1;
                break;
            case 'x': //Policies on multi-policy
                index = optind-1;
                pollist.clear();
                while(index<argc) {
                    next = strdup(argv[index]);
                    index++;
                    if(next[0] != '-') {
                        pollist.push_back(next);
                    }
                    else break;
                }
                optind = index - 1;
                break;
            case 'a': //Probability distribution
                if(optarg) arr = optarg;
                break;
        }
    }

    list<double> se;
    for (char* i: login) {
        se.push_back(atof(i));
    }       


    Simul sim = Simul(se, dist, k, arr);
    cout.precision(17);
    high_resolution_clock::time_point t1 = high_resolution_clock::now();

    if(type == 2) sim.simMD(time, lalist ,d , pollist,s);
    else sim.simulate(time, la, d,pollist, s);
    if (nr) {
        print_numeric(sim);
    }


    else print_result(sim);
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>( t2 - t1 ).count();
    cout << duration/1000000.0 << endl;
    return 0;
}
