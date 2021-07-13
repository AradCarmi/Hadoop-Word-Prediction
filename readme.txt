
number_of_instance: 2
ec2_instances: m4.large
hSadoop.version: 2.10.1
emr_version: emr-5.32.0

link to output file: s3://output-hadoop-test/outputfinal/part


------------------------------------How to run the project-------------------------

Run RunJob main class with params jar : s3://input-file-hadoop/<jarfile>, input : s3://input-file-hadoop/<input> 
,output: s3://output-hadoop-test/<output>
The RunJob main will run an EMR Cluster which has 5 steps that do the following:
Step 1- Sums up the occurrences by year in both parts of the corpus into a single file in the following format(tr: r0) ,(tr: r1).
Step 2- Combine the outcomes of Step1 into a single file in the following format: (Tr: r0 r1).Also sum the total number of trigram in the corpus (n) and upload it to s3 bucket.
Step 3- Calculate and append the Nr0 and Tr01 for each trigram in the Step2 output file.  format: (tr: r0 r1 tr01 nr0)
Step 4- Calculate and append the Nr1 and Tr10 for each trigram in the Step3 output file.  format: (tr: r0 r1 tr01 nr0 tr10 nr1)
Step 5- Calculate the Probability for each trigram in the output of Step4 which is the last step of the program.
final file format(tr: pr(tr)).
----------------------------------*Interesting Trigrams*---------------------------

-------------------------------אכפת לך Trigram------------------------------------ 

אכפת לך .	1.6722451814285212E-7
אכפת לך ?	7.188126477404688E-8
אכפת לך אם	1.0268893965762584E-7
אכפת לך מה	8.214684991071963E-8
אכפת לך שאני	3.667596224308859E-8


-------------------------------אכתוב לך Trigram------------------------------------ 

אכתוב לך .	1.1882409088271745E-7
אכתוב לך את	7.188126477404688E-8
אכתוב לך יותר	3.5208923753365046E-8
אכתוב לך מה	4.694523167115339E-8
אכתוב לך מכתב	6.748180278357322E-8
אכתוב לך עוד	6.307859396040948E-8
אכתוב לך על	1.6722451814285212E

-------------------------------בגלל שאין Trigram------------------------------------ 

בגלל שאין לה	1.7604461876682523E-8 בגלל שאין להם	2.7873731304747326E-8 בגלל שאין לו	6.601394507111965E-8 בגלל שאין לי	8.947967230303418E-8 בגלל שאין לנו	1.7604461876682523E-8 
-------------------------------בגלל שלא Trigram------------------------------------ 


בגלל שלא היה	1.3202521146782627E-7 בגלל שלא היו	9.975523140644139E-8 בגלל שלא היתה	4.40111546917063E-8 בגלל שלא רצה	3.227484677391796E-8 בגלל שלא רציתי	5.281338563004756E-8

-------------------------------בגלל תנאי Trigram------------------------------------ 

בגלל תנאי האקלים	4.694523167115339E-8 בגלל תנאי החיים	6.161561656838883E-8 בגלל תנאי המלחמה	4.107707771225922E-8 בגלל תנאי העבודה	3.8143000732812135E-8 בגלל תנאי מזג	4.40111546917063E-8

-------------------------------אחד המנהיגים Trigram------------------------------------ 

אחד המנהיגים הבולטים	1.0709380974981869E-7 אחד המנהיגים החשובים	3.5208923753365046E-8 אחד המנהיגים הציוניים	4.40111546917063E-8 אחד המנהיגים הרוחניים	3.0807808284194415E-8 אחד המנהיגים של	9.38848458681329E-8

-------------------------------אחד המרכיבים Trigram------------------------------------ 

אחד המרכיבים הבולטים	3.667596224308859E-8 אחד המרכיבים החשובים	2.464624662735553E-7 אחד המרכיבים המרכזיים	6.748180278357322E-8 אחד המרכיבים העיקריים	1.2614315641822366E-7 אחד המרכיבים של	2.02451311581849E-7

-------------------------------אחד הצדדים Trigram------------------------------------ 

אחד הצדדים .	3.5502331451309753E-7 אחד הצדדים או	6.454620475237879E-8 אחד הצדדים הלוחמים	3.0807808284194415E-8 אחד הצדדים לא	4.254411620198276E-8 אחד הצדדים של	3.667596224308859E-8

-------------------------------אכתי לא Trigram------------------------------------ 

אכתי לא הוה	4.547819318142985E-8
אכתי לא הוי	8.06836631587083E-8
אכתי לא היה	4.40111546917063E-8
אכתי לא ידע	3.667596224308859E-8
אכתי לא ידעינן	8.508017404309765E-8

-------------------------------אל אדוני Trigram------------------------------------ 

אל אדוני אבי	3.227484677391796E-8
אל אדוני המלך	9.828501948022541E-8
אל אדוני יש	5.281338563004756E-8
אל אדוני שעירה	2.420613508043847E-7
אל אדוני .	8.655094053192732E-8

-----------------------------------------Statics---------------------------------------


Step1-

	File System Counters
		FILE: Number of bytes read=121225985
		FILE: Number of bytes written=377503712
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=3627
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=39
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=2621736532
		S3: Number of bytes written=81258482
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Killed map tasks=1
		Launched map tasks=39
		Launched reduce tasks=1
		Data-local map tasks=39
		Total time spent by all maps in occupied slots (ms)=53322240
		Total time spent by all reduces in occupied slots (ms)=4210080
		Total time spent by all map tasks (ms)=1110880
		Total time spent by all reduce tasks (ms)=43855
		Total vcore-milliseconds taken by all map tasks=1110880
		Total vcore-milliseconds taken by all reduce tasks=43855
		Total megabyte-milliseconds taken by all map tasks=1706311680
		Total megabyte-milliseconds taken by all reduce tasks=134722560
	Map-Reduce Framework
		Map input records=83465129
		Map output records=83465128
		Map output bytes=1938833557
		Map output materialized bytes=247482313
		Input split bytes=3627
		Combine input records=83465128
		Combine output records=21257948
		Reduce input groups=3172863
		Reduce shuffle bytes=247482313
		Reduce input records=21257948
		Reduce output records=3172863
		Spilled Records=42515896
		Shuffled Maps =39
		Failed Shuffles=0
		Merged Map outputs=39
		GC time elapsed (ms)=23707
		CPU time spent (ms)=695150
		Physical memory (bytes) snapshot=33822687232
		Virtual memory (bytes) snapshot=134840287232
		Total committed heap usage (bytes)=29107421184

File System Counters
		FILE: Number of bytes read=117541907
		FILE: Number of bytes written=363529811
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=3534
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=38
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=2513049968
		S3: Number of bytes written=81229681
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Killed map tasks=1
		Launched map tasks=38
		Launched reduce tasks=1
		Data-local map tasks=38
		Total time spent by all maps in occupied slots (ms)=52676928
		Total time spent by all reduces in occupied slots (ms)=3177600
		Total time spent by all map tasks (ms)=1097436
		Total time spent by all reduce tasks (ms)=33100
		Total vcore-milliseconds taken by all map tasks=1097436
		Total vcore-milliseconds taken by all reduce tasks=33100
		Total megabyte-milliseconds taken by all map tasks=1685661696
		Total megabyte-milliseconds taken by all reduce tasks=101683200
	Map-Reduce Framework
		Map input records=80006835
		Map output records=80006835
		Map output bytes=1858473280
		Map output materialized bytes=237412377
		Input split bytes=3534
		Combine input records=80006835
		Combine output records=20379707
		Reduce input groups=3172856
		Reduce shuffle bytes=237412377
		Reduce input records=20379707
		Reduce output records=3172856
		Spilled Records=40759414
		Shuffled Maps =38
		Failed Shuffles=0
		Merged Map outputs=38
		GC time elapsed (ms)=23779
		CPU time spent (ms)=674020
		Physical memory (bytes) snapshot=32753164288
		Virtual memory (bytes) snapshot=131428712448
		Total committed heap usage (bytes)=28233957376
Step2

File System Counters
		FILE: Number of bytes read=56078680
		FILE: Number of bytes written=126434930
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=428
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=4
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=162506061
		S3: Number of bytes written=91295486
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Killed map tasks=1
		Launched map tasks=4
		Launched reduce tasks=1
		Data-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=4927104
		Total time spent by all reduces in occupied slots (ms)=1805664
		Total time spent by all map tasks (ms)=102648
		Total time spent by all reduce tasks (ms)=18809
		Total vcore-milliseconds taken by all map tasks=102648
		Total vcore-milliseconds taken by all reduce tasks=18809
		Total megabyte-milliseconds taken by all map tasks=157667328
		Total megabyte-milliseconds taken by all reduce tasks=57781248
	Map-Reduce Framework
		Map input records=6345719
		Map output records=6345719
		Map output bytes=186810791
		Map output materialized bytes=69258245
		Input split bytes=428
		Combine input records=0
		Combine output records=0
		Reduce input groups=3172967
		Reduce shuffle bytes=69258245
		Reduce input records=6345719
		Reduce output records=3172967
		Spilled Records=12691438
		Shuffled Maps =4
		Failed Shuffles=0
		Merged Map outputs=4
		GC time elapsed (ms)=2670
		CPU time spent (ms)=67930
		Physical memory (bytes) snapshot=4027146240
		Virtual memory (bytes) snapshot=18022395904
		Total committed heap usage (bytes)=3529506816
Step3-

File System Counters
		FILE: Number of bytes read=45487383
		FILE: Number of bytes written=91739368
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=224
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=2
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=91306211
		S3: Number of bytes written=133777440
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Killed map tasks=1
		Launched map tasks=2
		Launched reduce tasks=1
		Data-local map tasks=2
		Total time spent by all maps in occupied slots (ms)=2466480
		Total time spent by all reduces in occupied slots (ms)=1985568
		Total time spent by all map tasks (ms)=51385
		Total time spent by all reduce tasks (ms)=20683
		Total vcore-milliseconds taken by all map tasks=51385
		Total vcore-milliseconds taken by all reduce tasks=20683
		Total megabyte-milliseconds taken by all map tasks=78927360
		Total megabyte-milliseconds taken by all reduce tasks=63538176
	Map-Reduce Framework
		Map input records=3172967
		Map output records=3172967
		Map output bytes=93938584
		Map output materialized bytes=45593294
		Input split bytes=224
		Combine input records=0
		Combine output records=0
		Reduce input groups=6774
		Reduce shuffle bytes=45593294
		Reduce input records=3172967
		Reduce output records=3172967
		Spilled Records=6345934
		Shuffled Maps =2
		Failed Shuffles=0
		Merged Map outputs=2
		GC time elapsed (ms)=1995
		CPU time spent (ms)=46150
		Physical memory (bytes) snapshot=2649477120
		Virtual memory (bytes) snapshot=11367759872
		Total committed heap usage (bytes)=2303197184

Step4-

File System Counters
		FILE: Number of bytes read=49311794
		FILE: Number of bytes written=99283306
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=230
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=2
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=133792428
		S3: Number of bytes written=176259954
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Launched map tasks=2
		Launched reduce tasks=1
		Data-local map tasks=2
		Total time spent by all maps in occupied slots (ms)=2629488
		Total time spent by all reduces in occupied slots (ms)=2055840
		Total time spent by all map tasks (ms)=54781
		Total time spent by all reduce tasks (ms)=21415
		Total vcore-milliseconds taken by all map tasks=54781
		Total vcore-milliseconds taken by all reduce tasks=21415
		Total megabyte-milliseconds taken by all map tasks=84143616
		Total megabyte-milliseconds taken by all reduce tasks=65786880
	Map-Reduce Framework
		Map input records=3172967
		Map output records=3172967
		Map output bytes=136420228
		Map output materialized bytes=49312812
		Input split bytes=230
		Combine input records=0
		Combine output records=0
		Reduce input groups=6773
		Reduce shuffle bytes=49312812
		Reduce input records=3172967
		Reduce output records=3172967
		Spilled Records=6345934
		Shuffled Maps =2
		Failed Shuffles=0
		Merged Map outputs=2
		GC time elapsed (ms)=1581
		CPU time spent (ms)=51510
		Physical memory (bytes) snapshot=2729062400
		Virtual memory (bytes) snapshot=11358625792
		Total committed heap usage (bytes)=2363490304

Step5-

File System Counters
		FILE: Number of bytes read=53409444
		FILE: Number of bytes written=102163833
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=345
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=3
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
		S3: Number of bytes read=176289862
		S3: Number of bytes written=138765621
		S3: Number of read operations=0
		S3: Number of large read operations=0
		S3: Number of write operations=0
	Job Counters 
		Killed map tasks=1
		Launched map tasks=3
		Launched reduce tasks=1
		Data-local map tasks=3
		Total time spent by all maps in occupied slots (ms)=3502992
		Total time spent by all reduces in occupied slots (ms)=2205792
		Total time spent by all map tasks (ms)=72979
		Total time spent by all reduce tasks (ms)=22977
		Total vcore-milliseconds taken by all map tasks=72979
		Total vcore-milliseconds taken by all reduce tasks=22977
		Total megabyte-milliseconds taken by all map tasks=112095744
		Total megabyte-milliseconds taken by all reduce tasks=70585344
	Map-Reduce Framework
		Map input records=3172967
		Map output records=3172967
		Map output bytes=176259954
		Map output materialized bytes=47875548
		Input split bytes=345
		Combine input records=0
		Combine output records=0
		Reduce input groups=3172967
		Reduce shuffle bytes=47875548
		Reduce input records=3172967
		Reduce output records=3172967
		Spilled Records=6345934
		Shuffled Maps =3
		Failed Shuffles=0
		Merged Map outputs=3
		GC time elapsed (ms)=2120
		CPU time spent (ms)=64790
		Physical memory (bytes) snapshot=3459653632
		Virtual memory (bytes) snapshot=14685036544
		Total committed heap usage (bytes)=3101163520