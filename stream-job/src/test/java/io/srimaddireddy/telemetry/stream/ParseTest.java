package io.srimaddireddy.telemetry.stream;

import io.srimaddireddy.telemetry.model.AdEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ParseTest {
    @Test
    @SuppressWarnings("unchecked")
    void nullJsonGoesToDeadLetterInsteadOfCrashingParser() throws Exception {
        var parser = new TelemetryJob.Parse();
        ProcessFunction<String, AdEvent>.Context context =
                mock(ProcessFunction.Context.class);
        Collector<AdEvent> output = mock(Collector.class);

        parser.processElement("null", context, output);

        verify(context).output(eq(TelemetryJob.INVALID), contains("INVALID_EVENT"));
        verifyNoInteractions(output);
    }
}
