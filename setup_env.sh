
source /sdf/group/lcls/ds/ana/sw/conda2-v4/inst/etc/profile.d/conda.sh


export CONDA_ENVS_DIRS=/sdf/group/lcls/ds/ana/sw/conda_bld/kmecseki/.conda/envs
conda activate ejfat-dev


export LD_LIBRARY_PATH="/sdf/home/k/kmecseki/opt/grpc2/lib:/sdf/home/k/kmecseki/opt/grpc2/lib64/:$LD_LIBRARY_PATH"

#export EJFAT_URI="ejfat://local-test@127.0.0.1:9876/lb/1?data=127.0.0.1:19522&sync=127.0.0.1:19523"

