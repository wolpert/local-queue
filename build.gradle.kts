// For publishing, all of these are required in the top-level.

plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}
nexusPublishing {
    repositories {
        sonatype()
    }
}
allprojects {
    group = "com.codeheadsystems"
    version = "1.0.2"
    //version = "1.0.1-SNAPSHOT"
}
// gradle clean build test publishToSonatype closeAndReleaseSonatypeStagingRepository
