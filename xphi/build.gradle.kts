import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.io.File

plugins {
    id("org.springframework.boot") version "3.2.2"
    id("io.spring.dependency-management") version "1.1.4"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.spring") version "2.1.0"
}

group = "xphi"
version = "1.0.3-001"

// 1. JAR 파일명 설정 (xphi-1.0.3-001.jar)
base {
    archivesName.set("${project.group}")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

// 상위 폴더를 탐색하여 self를 찾는 함수
fun findAnchorDirectory(): File? {
    var current: File? = projectDir
    // 현재 위치부터 상위로 3단계까지 탐색
    for (i in 1..3) {
        current = current?.parentFile
        if (current == null) break

        val target = File(current, "self")
        if (target.exists() && target.isDirectory) {
            return target
        }
    }
    return null
}

// 탐색 결과 저장
val anchorDir = findAnchorDirectory()
val boundJsonFile = anchorDir?.let { File(it, "bound.json") }

var resolver = tasks.register("resolver") {
    group = "verification"
    description = "resolver.py 스크립트를 실행하여 로그를 출력"

    doLast {
        val scriptFile = anchorDir?.let { File(it, "resolver.py") }

        if (scriptFile != null && scriptFile.exists()) {
            logger.lifecycle(">> [Python Exec] Running: ${scriptFile.absolutePath}")
            exec {
                commandLine("python3", scriptFile.absolutePath, "--around")
                standardOutput = System.`out`
                errorOutput = System.`err`
            }
        } else {
            logger.warn(">> [Python Skip] self.py를 찾을 수 없습니다. (경로 확인 필요)")
        }
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.3")
    implementation("org.jetbrains.kotlin:kotlin-compiler-embeddable:2.1.0")
    implementation("com.google.code.gson:gson:2.10.1")

    // xor / junction 관련 의존성
    implementation("org.apache.lucene:lucene-core:9.10.0")
    implementation("org.apache.lucene:lucene-analysis-common:9.10.0")
    implementation("org.apache.lucene:lucene-queryparser:9.10.0")
    implementation("org.yaml:snakeyaml:2.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("io.arrow-kt:arrow-core:1.2.4")
    implementation("io.arrow-kt:arrow-fx-coroutines:1.2.4")

    runtimeOnly("io.netty:netty-resolver-dns-native-macos:4.1.108.Final:osx-aarch_64")
}

// 2. 리소스 처리: bound/anchor/bound.json을 JAR 내부에 포함
tasks.named<ProcessResources>("processResources") {
    if (boundJsonFile != null && boundJsonFile.exists()) {
        from(boundJsonFile) {
            into("external-config") // JAR 내부 /external-config/ 폴더에 저장
        }
        doFirst {
            logger.lifecycle(">> [Resource] Found and including: ${boundJsonFile.absolutePath}")
        }
    } else {
        doFirst {
            logger.warn(">> [Resource] Warning: bound.json not found in upper directories.")
        }
    }
}

// 3. 빌드 완료 후 JAR 파일을 bound/anchor/lib로 복사하는 태스크
val copyJarToAnchor = tasks.register<Copy>("copyJarToAnchor") {
    group = "distribution"
    description = "Copies the generated bootJar to bound/anchor/cache/lib"

    if (anchorDir != null) {
        val targetLibDir = File(anchorDir, "cache/lib")

        // bootJar 결과물을 소스로 지정
        from(tasks.named("bootJar"))
        into(targetLibDir)

        doLast {
            logger.lifecycle(">> [Success] JAR copied to: ${targetLibDir.absolutePath}")
        }
    } else {
        doLast {
            logger.error(">> [Error] Target 'bound/anchor' directory not found. Copy skipped.")
        }
    }
}

// bootJar 실행 후 자동으로 복사 태스크 실행
tasks.named("bootJar") {
    finalizedBy(copyJarToAnchor)
    finalizedBy(resolver)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "21"
        freeCompilerArgs = listOf("-Xjsr305=strict")
    }
}