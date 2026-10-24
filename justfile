run file:
    go run main.go {{ file }}

build:
    go build .
clean:
    rm -f Big_Buck_Bunny_360_10s_1MB.mp4 Big_Buck_Bunny_1080_10s_5MB.mp4 500MB-CZIPtestfile.org.zip 3dicons-png-dynamic-1.0.0.zip


verify select:
    {{ 
        if select == "1" 
        { 
            "md5sum ./Big_Buck_Bunny_360_10s_1MB.mp4 ./verify_file/Big_Buck_Bunny_360_10s_1MB.mp4" 
        } else { 
            "md5sum ./Big_Buck_Bunny_1080_10s_5MB.mp4 ./verify_file/Big_Buck_Bunny_1080_10s_5MB.mp4" 
        } 
    }}
