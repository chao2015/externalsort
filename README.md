# externalsort
The single and cluster versions of external sorting are implemented based on Golang.

---

Details:

[https://blog.csdn.net/chao2016/article/details/81638592](https://blog.csdn.net/chao2016/article/details/81638592)



Tree:

```
externalsort
├── README.md
├── main
│   ├── cluster_sort.go
│   ├── generate.go
│   └── single_sort.go
├── pipeline
│   ├── common.go
│   ├── net_nodes.go
│   └── nodes.go
└── test
    └── pipeline_test.go
```

Downloads:

```
git clone git@github.com:chao2015/externalsort.git

```

Run:

```
mv externalsort/ $GOPATH/src/
cd $GOPATH/src/externalsort/main/
go run generate.go 
go run single_sort.go 
go run cluster_sort.go 
```
Have fun! ^_^
