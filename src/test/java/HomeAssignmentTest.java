import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.Mockito.atLeastOnce;

class HomeAssignmentTest {
    @InjectMocks
    private HomeAssignment homeAssignment;

    @Mock
    private SparkSession sparkSession;

    @Test
    void mainTest() {

        HomeAssignment.main(null);

        Mockito.verify(homeAssignment, atLeastOnce()).sparkProcessing(sparkSession);
    }
}