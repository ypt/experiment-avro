import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.72"
    id("com.commercehub.gradle.plugin.avro") version "0.21.0"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.avro:avro:1.10.0")
    implementation("org.apache.parquet:parquet-avro:1.11.0")
    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    // implementation("org.apache.hadoop:hadoop-core:3.2.1")
    // implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.2.1")
    // implementation("org.apache.hadoop:hadoop-client:3.2.1")
}

tasks.withType<Jar>() {
    manifest {
        attributes["Main-Class"] = "com.example.demo.ApplicationKt"
    }
    configurations["compileClasspath"].forEach { file ->
        from(zipTree(file.absoluteFile))
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}
