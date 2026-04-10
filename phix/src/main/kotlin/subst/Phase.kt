package subst

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan("subst.xor")
class Phase

fun main(args: Array<String>) {
    runApplication<Phase>(*args)
}