scp indianapolis:/home/s1687259/drf-sc-exec/src/drf-sc/cLogs/client* ./ ;
scp austin:/home/s1687259/drf-sc-exec/src/drf-sc/cLogs/client* ./  ;
scp sanantonio:/home/s1687259/drf-sc-exec/src/drf-sc/cLogs/client* ./ ;
scp philly:/home/s1687259/drf-sc-exec/src/drf-sc/cLogs/client* ./ ;

cat client* | sort | uniq -d ;
