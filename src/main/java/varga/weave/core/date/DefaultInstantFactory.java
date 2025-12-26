package varga.weave.core.date;


import java.time.Instant;

public class DefaultInstantFactory implements InstantFactory {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
