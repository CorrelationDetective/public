package _aux;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class Stage {
    @NonNull @Getter @Setter public String name;
    @NonNull @Getter @Setter public Double duration;
    @Getter @Setter public Double expectedDuration;

}