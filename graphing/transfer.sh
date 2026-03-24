rsync -av   --include='*/'   --include='*.csv'   --include='*.npy' --include='*.out' --include='submit.sh' --exclude='*'   aurora:/lus/flare/projects/AuroraGPT/sockerman/cleanEvalPaperResults/insert/ ./insert/
rsync -av   --include='*/'   --include='*.csv'   --include='*.npy' --include='*.out' --include='submit.sh' --exclude='*'   polaris:/eagle/projects/argonne_tpc/sockerman/evalPaperResults/insert/ ./insert/

rsync -av   --include='*/'   --include='*.csv'   --include='*.txt' --include='*.out' --include='submit.sh' --exclude='*'   aurora:/lus/flare/projects/AuroraGPT/sockerman/cleanEvalPaperResults/index/ ./aurora/
rsync -av   --include='*/'   --include='*.csv'   --include='*.txt' --include='*.out' --include='submit.sh' --exclude='*'   polaris:/eagle/projects/argonne_tpc/sockerman/evalPaperResults/index/ ./index/


rsync -av   --include='*/'   --include='*.csv'   --include='*.txt' --include='*.out' --include='submit.sh' --include='*.go' --exclude='*'   aurora:/lus/flare/projects/AuroraGPT/sockerman/cleanEvalPaperResults/query/ ./aurora/
rsync -av   --include='*/'   --include='*.csv'   --include='*.txt' --include='*.out' --include='submit.sh' --exclude='*'   polaris:/eagle/projects/argonne_tpc/sockerman/evalPaperResults/index/ ./index/