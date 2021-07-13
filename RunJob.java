import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.ArrayList;
import java.util.Collection;


public class RunJob {

    private static Region region = Region.of("us-east-1");
    private static EmrClient emr =  EmrClient.builder().region(region).build();

    public static void main(String[] args) {
        HadoopJarStepConfig hadoopJarStepr0 = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step1.jar")
                .args("s3://input-file-hadoop/part1",
                        "s3://output-hadoop-test/outputr0")
                .build();
        HadoopJarStepConfig hadoopJarStepr1 = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step1.jar")
                .args("s3://input-file-hadoop/part2",
                        "s3://output-hadoop-test/outputr1")
                .build();

        HadoopJarStepConfig hadoopJarStepcombr0r1 = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step2.jar")
                .args("s3://output-hadoop-test/outputr0/1-r-00000",
                        "s3://output-hadoop-test/outputr1/2-r-00000",
                        "s3://output-hadoop-test/outputr0r1")
                .build();
        HadoopJarStepConfig hadoopJarStepnr0tr01 = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step3.jar")
                .args("s3://output-hadoop-test/outputr0r1/part-r-00000",
                        "s3://output-hadoop-test/outputnr0tr01")
                .build();

        HadoopJarStepConfig hadoopJarStepnr1tr10 = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step4.jar")
                .args("s3://output-hadoop-test/outputnr0tr01/part-r-00000",
                        "s3://output-hadoop-test/outputnr1tr10")
                .build();

        HadoopJarStepConfig hadoopJarStepfinal = HadoopJarStepConfig.builder()
                .jar("s3://input-file-hadoop/Step5.jar")
                .args("s3://output-hadoop-test/outputnr1tr10/part-r-00000",
                        "s3://output-hadoop-test/outputfinal")
                .build();


        StepConfig stepr0 = StepConfig.builder()
                .name("r0")
                .hadoopJarStep(hadoopJarStepr0)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        StepConfig stepr1 = StepConfig.builder()
                .name("r1")
                .hadoopJarStep(hadoopJarStepr1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        StepConfig stepr0r1 = StepConfig.builder()
                .name("r0r1")
                .hadoopJarStep(hadoopJarStepcombr0r1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        StepConfig stepnr0tr01 = StepConfig.builder()
                .name("nr0tr01")
                .hadoopJarStep(hadoopJarStepnr0tr01)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        StepConfig stepnr1tr10 = StepConfig.builder()
                .name("nr1tr10")
                .hadoopJarStep(hadoopJarStepnr1tr10)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();


        StepConfig stepfinal = StepConfig.builder()
                .name("final")
                .hadoopJarStep(hadoopJarStepfinal)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        ArrayList<StepConfig> steps_config = new ArrayList<>();
        steps_config.add(stepr0);
        steps_config.add(stepr1);
        steps_config.add(stepr0r1);
        steps_config.add(stepnr0tr01);
        steps_config.add(stepnr1tr10);
        steps_config.add(stepfinal);

        JobFlowInstancesConfig instances =JobFlowInstancesConfig.builder()
                .instanceCount(2)
                .masterInstanceType("m4.large")
                .slaveInstanceType("m4.large")
                .hadoopVersion("2.10.1")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();


        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("3gram word prob")
                .instances(instances)
                .releaseLabel("emr-5.32.0")
                .steps(steps_config)
                .logUri("s3://emr-logging-test")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .build();

        RunJobFlowResponse runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }


}
