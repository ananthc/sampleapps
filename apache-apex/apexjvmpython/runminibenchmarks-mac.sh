#!/bin/bash
#java -Djava.library.path=/Users/Ananth/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.:/usr/local/lib/python3.5/site-packages/jep -jar target/benchmarks.jar -wi 3 -i 3 -f 3 ".*XGBoostJepBenchMarkDepth3.*"
#java -Djava.library.path=/Users/Ananth/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.:/usr/local/lib/python3.5/site-packages/jep -jar target/benchmarks.jar -wi 3 -i 3 -f 3 ".*XGBoostJepBenchMarkDepth9.*"
#java -Djava.library.path=/Users/Ananth/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.:/usr/local/lib/python3.5/site-packages/jep -jar target/benchmarks.jar -wi 3 -i 3 -f 3 ".*XGBoostJepBenchMarkDepth27.*"
java -Djava.library.path=/Users/Ananth/Library/Java/Extensions:/Library/Java/Extensions:/Network/Library/Java/Extensions:/System/Library/Java/Extensions:/usr/lib/java:.:/usr/local/lib/python3.5/site-packages/jep -jar target/benchmarks.jar -wi 5 -i 5 -f 5 ".*XGBoostJepBenchMarkDepth125.*"