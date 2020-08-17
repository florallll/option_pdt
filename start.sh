nohup python service/web_connect.py > logs/web_connect.log 2>&1 &
nohup ~/q/l32/q scripts/datasaver.q > logs/datasaver.log 2>&1 &
nohup python main.py -f delta_strategy> logs/delta.log 2>&1 &
nohup python main.py -f gamma_vega> logs/gamma_vega.log 2>&1 &
nohup node nodeServer.js > ~/repos/pdt_option/logs/nodeServer.log 2>&1 &

