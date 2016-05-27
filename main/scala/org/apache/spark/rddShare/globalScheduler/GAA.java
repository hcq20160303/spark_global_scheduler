package org.apache.spark.rddShare.globalScheduler;

/**
 * Created by hcq on 16-5-26.
 */

import java.util.Random;
import java.util.Arrays;
import java.lang.Math;

public class GAA{
    private int scale; //种群规模
    private int DAG_num; //DAG的数目，需要提前进行统计
    private int Max_len; //最大迭代次数
    private int Now_len; //当前迭代次数
    /**
     * questions 2: It's may be more suitable to using percent to represent the reuse matrix
     */
    private int [][]reuse; //重用度矩阵，需要提前统计
    private int [] Best_result; //最佳个体
    private int [][] Best_seq; //最佳个体序列
    private float [] Best_fit; //最佳个体序列对应的适应度

    private int [][] Parent_population; //父种群
    private int [][] Child_population; //子种群

    private float Wr; //重用度占适应度的权值
    private float Wl; //位置关系占适应度的权值
    private float [] fitness; //适应度
    private float Ps; //选择概率
    private float Pc; //交叉概率
    private float Pm; //变异概率

    private Random random;

    //构造函数
    public GAA(int s,int D,int M,float S,float c,float m,float r,float l){
        scale=s;
        DAG_num=D;
        Max_len=M;
        Ps=S;
        Pc=c;
        Pm=m;
        Wr=r;
        Wl=l;
    }

    //初始GA
    void init(){
        reuse=new int [DAG_num][DAG_num];
//        reuse=statis(); //Statis()是用来计算重用度矩阵的函数，需要在该类之外的地方编写，返回的是重用度矩阵

        //初始化一些参数
        Now_len=0;
        fitness=new float[scale];
        Best_result=new int[DAG_num];
        Best_seq=new int[Max_len][DAG_num];
        Best_fit=new float[Max_len];

        Parent_population=new int[scale][DAG_num];
        Child_population=new int[scale][DAG_num];

        random = new Random(System.currentTimeMillis());
    }

    //构造初始化种群
    void initPopulation(){
        int [] sequece;
        sequece= new int [DAG_num];
        for(int i=0;i<DAG_num;i++){
            sequece[i]=i;
        }
        for(int i=0;i<scale;i++){
            int end = DAG_num-1;
            for(int j=0;j<DAG_num;j++){
                int r=random.nextInt(end+1);
                Parent_population[i][j]=sequece[r];
                sequece[r]=sequece[end];
                end--;
            }
        }
    }

    //计算每个个体的重用度的大小
    /**
     * @param individual
     * @return
     */
    int caclReuse(int [] individual){
        int sum_reuse=0;
        for(int i=0;i<DAG_num-1;i++){
            for(int j=i+1;j<DAG_num;j++){
                /**
                 * question 1: the sum reuse, I think, can't sum all of the reuse of a dag to other dags,
                 * just sum the biggest reuse is OK?
                 */
                sum_reuse=sum_reuse+reuse[individual[i]][individual[j]];
            }
        }
        return sum_reuse;
    }

    //计算每个个体中的基因的位置关系，考虑到Spark的并行执行
    int caclLocation(int [] individual){
        int sum_location=0;
        for(int i=0;i<DAG_num-1;i++){
            for(int j=i+1;j<DAG_num;j++){
                if(reuse[individual[i]][individual[j]]!=0)
                    sum_location=sum_location+j-i;
            }
        }
        return sum_location;
    }

    //计算每个个体的适应度，个体适应度是由重用度和位置共同决定的
    void caclfitness(){
        int [] reuse_queue;
        reuse_queue=new int[scale];
        int [] location_queue;
        location_queue=new int[scale];
        for(int i=0;i<scale;i++){
            reuse_queue[i]=caclReuse(Parent_population[i]);
            location_queue[i]=caclLocation(Parent_population[i]);
        }
        for (int j=0;j<scale;j++){
            fitness[j]=reuse_queue[j]*Wr+location_queue[j]*Wl;
        }
    }

    //选择种群中的个体，按照一定概率保留适应度大的个体，且随机填充余下的个体，产生下一代种群
    void select(){
        float [] temp_queue;
        temp_queue=fitness; //将适应度数组先保存起来，为了确定需要保存的个体
        Arrays.sort(fitness);  //将适应度按照从小到大排序
        for(int i=scale-1;i>=scale*(1-Ps);i--){
            for(int j=0;j<scale;j++){
                /**
                 * question 4: what's the meaning?
                 */
                if(temp_queue[j]==fitness[i]){
                    Child_population[scale-1-i]=Parent_population[j];
                    if(i==scale-1){
                        /**
                         * question 5: Now_len++? why this inc is not after this statement:
                         * Best_fit[Now_len]=fitness[i];
                         */
                        Best_seq[Now_len++]=Parent_population[j];
                        Best_fit[Now_len]=fitness[i];
                    }
                }
            }
        }
        for(int i=(int)(scale*Ps);i>=0;i--){
            /**
             * question 3: why use child_population init itself? why not parent_population
             */
            Child_population[i]=Child_population[random.nextInt((int)(scale*Ps))+(int)(scale*(1-Ps))+1];
        }
    }

    //旋转个体中的基因，用于启发式交叉，为右旋转，已经排完序的基因不需要再参与旋转
    int [][] rotate(int [][] new_population,int row,int column,int num){
        int [][] temp_array;
        temp_array=new_population;
        System.arraycopy(new_population[row],num,temp_array[row],column,DAG_num-column);
        System.arraycopy(new_population[row],DAG_num-column,temp_array[row],num,column-num);
        return new_population;
    }

    //对新的种群进行交叉处理,采用两交换启发式交叉法
    void cross(){
        int ran1,ran2,temp,flag1=0,flag2=0;
        float max1=0,max2=0;

        int [][] temp_array;
        int [][] fit_Rarray,fit_Larray;
        float [][] fit;
        fit_Rarray=new int[2][DAG_num];
        fit_Larray=new int[2][DAG_num];
        fit=new float [2][DAG_num];
        temp_array=Child_population;

        for (int i=0;i<DAG_num;i++){
            fit_Rarray[0][i]=0;
            fit_Larray[1][i]=0;
        }

        for(int i=0;i<scale*Pc;i++){
            ran1=random.nextInt(DAG_num); //随机挑选两个个体
            ran2=random.nextInt(DAG_num);
            while(temp_array[ran1]==temp_array[ran2]){ //保证两个个体不想等
                ran2=random.nextInt(DAG_num);
            }
            if (fitness[ran2] > fitness[ran1]) { //保证ran1的适应度要大于ran2的适应度，完成交叉后，ran1个体不变，ran2个体变为两个交叉后的个体
                temp=ran1;
                ran1=ran2;
                ran2=temp;
            }

            for(int j=0;j<DAG_num;j++){
                for(int k=0;k<DAG_num&&j!=k;k++){
                    fit_Rarray[0][j]=fit_Rarray[0][j]+reuse[temp_array[ran1][j]][temp_array[ran1][k]];  //计算重用度
                    fit_Rarray[1][j]=fit_Rarray[1][j]+reuse[temp_array[ran2][j]][temp_array[ran2][k]];
                    if(reuse[temp_array[ran1][j]][temp_array[ran1][k]]!=0)  //计算位置关系
                        fit_Larray[0][j]=fit_Larray[0][j]+Math.abs(j-k);
                    if(reuse[temp_array[ran2][j]][temp_array[ran2][k]]!=0)
                        fit_Larray[1][j]=fit_Larray[1][j]+Math.abs(j-k);
                }
                fit[0][j]=fit_Rarray[0][j]*Wr+fit_Larray[0][j]*Wl; //计算个体基因j的适应度大小，按照从大到小排列
                fit[1][j]=fit_Rarray[0][j]*Wr+fit_Larray[0][j]*Wl;
                if(fit[0][j]>max1){
                    max1=fit[0][j];
                    flag1=j;  //记下适应度最大的基因的下标
                }
                if(fit[1][j]>max2){
                    max2=fit[1][j];
                    flag2=j;
                }
            }

            if(max1>=max2){
                Child_population[ran2][i]=temp_array[ran1][i]; //记下交叉后的个体基因
                rotate(temp_array,ran2,flag2,i); //对基因适应度小的个体进行旋转操作
            }
            else{
                Child_population[ran2][i]=temp_array[ran2][i];
                rotate(temp_array,ran1,flag1,i);
            }
        }
    }

    //对种群进行变异处理，对个体之间的基因进行互换位置
    void mutate(){
        int [] label;
        label=new int[(int)(scale*Pm)];
        int ran1,ran2,temp;
        for(int i=0;i<scale*Pm;i++){
            label[i]=random.nextInt(scale);  //根据个体数目随机选择需要变异的个体，注意种群中变异个体数目为scale*Pm
            for (int j=0;j<i;j++){           //对已经进行过变异的个体不再进行变异处理
                if(label[i]==label[j]){
                    label[i]=random.nextInt(DAG_num);
                    j=0;
                }
            }
            ran1=random.nextInt(DAG_num);
            ran2=random.nextInt(DAG_num);
            while(ran1==ran2){              //要求对个体的不同基因进行交换
                ran2=random.nextInt(DAG_num);
            }
            temp =Child_population[label[i]][ran1];
            Child_population[label[i]][ran1]=Child_population[label[i]][ran2];
            Child_population[label[i]][ran2]=temp;
        }
    }

    //进行迭代，算出最佳个体
    void iteration(){
        float [] temp;

        init();
        initPopulation();
        for(int i=0;i<Max_len;i++){
            caclfitness();
            select();
            cross();
            mutate();
            //将新的后代复制到父代，进行下一轮的迭代
            for(int j=0;j<scale;j++){
                System.arraycopy(Child_population[j],0,Parent_population[j],0,scale*DAG_num);
            }
        }
        temp=Best_fit;
        Arrays.sort(Best_fit);
        for(int i=0;i<Max_len;i++){
            if (temp[i]==Best_fit[Max_len-1]){
                Best_result=Best_seq[i];
            }
        }
    }

}
