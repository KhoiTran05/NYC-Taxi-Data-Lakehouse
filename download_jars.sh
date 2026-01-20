#!/bin/bash
# download-connect-jars.sh

set -e

echo "Creating directory for JARs..."
mkdir -p ./connect-jars
cd ./connect-jars

echo "Downloading Iceberg Kafka Connect JARs..."
echo ""

# Counters
successful=0
failed=0

# Function to download with error handling
download_if_missing() {
    local url=$1
    local filename=$(basename "$url")
    
    if [ -f "$filename" ]; then
        echo "⏭  Skipping $filename (already exists)"
        ((successful++))
        return 0
    fi
    
    echo "⬇  Downloading $filename..."
    if curl -fsSL -o "$filename" "$url" 2>/dev/null; then
        echo "✓  Downloaded $filename"
        ((successful++))
        return 0
    else
        echo "✗  Failed to download $filename"
        echo "   URL: $url"
        ((failed++))
        return 1
    fi
}

# Array of download URLs
declare -a urls=(
    # Core Iceberg Kafka Connect
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect/1.8.1/iceberg-kafka-connect-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect-events/1.8.1/iceberg-kafka-connect-events-1.8.1.jar"
    
    # Core Iceberg libraries
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.8.1/iceberg-core-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/1.8.1/iceberg-api-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-common/1.8.1/iceberg-common-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-bundled-guava/1.8.1/iceberg-bundled-guava-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-parquet/1.8.1/iceberg-parquet-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.8.1/iceberg-aws-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.8.1/iceberg-nessie-1.8.1.jar"
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar"
    
    # Nessie catalog
    "https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-client/0.77.1/nessie-client-0.77.1.jar"
    "https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-model/0.77.1/nessie-model-0.77.1.jar"
    
    # AWS SDK v2
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.100/bundle-2.20.100.jar"
    "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.100/url-connection-client-2.20.100.jar"
    
    # Parquet dependencies
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-column/1.12.3/parquet-column-1.12.3.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.12.3/parquet-hadoop-1.12.3.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.12.3/parquet-avro-1.12.3.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-common/1.12.3/parquet-common-1.12.3.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-encoding/1.12.3/parquet-encoding-1.12.3.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-format-structures/1.12.3/parquet-format-structures-1.12.3.jar"
    
    # Additional dependencies
    "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.1/avro-1.11.1.jar"
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar"
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar"
    "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar"
    "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
    "https://repo1.maven.org/maven2/commons-io/commons-io/2.11.0/commons-io-2.11.0.jar"
    "https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.13/httpclient-4.5.13.jar"
    "https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.14/httpcore-4.4.14.jar"
    "https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar"
)

# Download each JAR
for url in "${urls[@]}"; do
    download_if_missing "$url" || true  # Continue even if download fails
done

echo ""
echo "========================================"
echo "Download Summary:"
echo "  Successful: $successful"
echo "  Failed: $failed"
echo "========================================"

if [ $failed -gt 0 ]; then
    echo ""
    echo "⚠  Some downloads failed. This might be okay if those JARs aren't strictly required."
    echo "   Try building the Docker image to see if it works with the downloaded JARs."
fi

echo ""
echo "JARs location: $(pwd)"
echo ""
ls -lh *.jar 2>/dev/null | awk '{printf "%-60s %10s\n", $9, $5}' || echo "No JARs found"

exit 0