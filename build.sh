# -- Software Stack Version
instruqt=0

for arg in "$@"
do
  case $arg in
     --instruqt)
     instruqt=1
     shift
     ;;
  esac
done

if [ $instruqt == 1 ]
then
  echo "Running instruqt Docker build"
  cd docker
else
  echo "Running local Docker build"
  cd docker-local
fi

./build.sh