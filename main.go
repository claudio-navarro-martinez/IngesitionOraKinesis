// V$LOGMNR_CONTENTS contains log history information. To query this view, you must have the SELECT ANY TRANSACTION privilege.
// SCN  System change number (SCN) when the database change was made
//START_SCN  System change number (SCN) when the transaction that contains this change started;
//           only meaningful if the COMMITTED_DATA_ONLY option was chosen in a DBMS_LOGMNR.START_LOGMNR() invocation, NULL otherwise.
//           This column may also be NULL if the query is run over a time/SCN range that does not contain the start of the transaction.
// select current_scn from v$database;
//

package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	goracle "github.com/godror/godror"
)

type lcs struct {
	Scn       int64
	Sql_redo  []string
	Committed bool
	TableName string
	Owner     string
}

var SCNDesde int64
var SCNHasta int64

type txs struct {
	XidCommitted string
	LastSCN      int
}

var lock sync.Mutex
var auxLCS lcs

func main() {
	var m = make(map[string]lcs)
	var n = make(map[string]lcs)
	var x int64
	var operationCode int
	var xid []byte
	var scn, scnmax, scnmin int64
	var TableName, Owner string
	var sqlRedo string
	var q2, qmaxmin string

	SCNDesdeDir := flag.Int64("scn", 0, "SCN desde donde quieres que empieze a leer")
	flag.Parse()
	SCNDesde = *SCNDesdeDir
	SCNHasta = SCNDesde + 100000

	var testConStr string
	var P goracle.ConnectionParams
	P.Username = "SYS as SYSDBA"
	P.Password = goracle.NewPassword("austral1a")
	P.ConnectString = "192.168.0.171:1521/ORCLCDB?connect_timeout=2"
	P.SessionTimeout = 42 * time.Second
	P.WaitTimeout = 10 * time.Second
	P.MaxLifeTime = 5 * time.Minute
	P.SessionTimeout = 30 * time.Second
	P.ConnClass = "POOLED"

	if strings.HasSuffix(strings.ToUpper(P.Username), " AS SYSDBA") {
		P.IsSysDBA, P.Username = true, P.Username[:len(P.Username)-10]
	}
	// P.ConnClass = goracle.NoConnectionPoolingConnectionClass
	testConStr = P.StringWithPassword()
	// testConStr = strings.Replace(testConStr, "POOLED", goracle.NoConnectionPoolingConnectionClass, 1)
	fmt.Println(testConStr)
	db, err := sql.Open("godror", testConStr)
	defer db.Close()

	if err != nil {
		fmt.Println(err, testConStr)
	}

	// if err := LoadMap("./mimapa.json", &m); err != nil {
	// 	fmt.Println(err)
	// }

	fmt.Println(x)
	if _, err := db.Exec("BEGIN DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo01.log',    OPTIONS => DBMS_LOGMNR.NEW);     END;"); err != nil {
		fmt.Println(err, "res de res")
	}
	if _, err := db.Exec("BEGIN DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo02.log',    OPTIONS => DBMS_LOGMNR.ADDFILE); END;"); err != nil {
		fmt.Println(err, "res de res")
	}
	if _, err := db.Exec("BEGIN DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo03.log',    OPTIONS => DBMS_LOGMNR.ADDFILE); END;"); err != nil {
		fmt.Println(err, "res de res")
	}

	// operativa normal
	for {
		// sacamos el ultimo SCN commiteado del fichero
		if err := LoadXid("./mitxt.json", &x); err != nil {
			if SCNDesde == 0 {
				fmt.Println("fichero mitxt.json no existe y no has pasado el SCN de punto de partida via -scn flag ")
				return
			}
		} else {
			SCNDesde = x + 1
			SCNHasta = SCNDesde + 100000
		}
		SCNDesde = 2794474
		SCNHasta = 2886507
		// creamos la view v$logmnr_contents
		fmt.Println("BEGIN dbms_logmnr.start_logmnr(startscn=>" + strconv.FormatInt(SCNDesde, 10) + ",endscn=>" + strconv.FormatInt(SCNHasta, 10) + " , options=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;")
		if _, err := db.Exec("BEGIN dbms_logmnr.start_logmnr(startscn=>" + strconv.FormatInt(SCNDesde, 10) + ",endscn=>" + strconv.FormatInt(SCNHasta, 10) + " , options=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;"); err != nil {
			fmt.Println(err, "res1 de res2")
		}
		// calculamos min scn,max scn dentro de SCNdesde,SCNHasta
		qmaxmin = "select min(scn),max(scn) from v$logmnr_contents"
		err := db.QueryRow(qmaxmin, 1).Scan(&scnmin, &scnmax)
		if err != nil {
			fmt.Println("la query no devuelve ninguna fila, probablemente v$ este vacia")
			if err := SaveXid("./mitxt.json", SCNHasta); err != nil {
				fmt.Println(err)
			}
		}
		fmt.Println(scnmin, scnmax)

		q2 = "SELECT scn, sql_redo,operation_code,xid,table_name,seg_owner from v$logmnr_contents where scn >= " +
			fmt.Sprint(scnmin) + " and scn <= " + fmt.Sprint(scnmax) + " order by scn,xid "
		fmt.Println(q2)
		rows, err := db.Query(q2)
		defer rows.Close()
		if err != nil {
			fmt.Println("Error running query 2")
			fmt.Println(err)
			return
		}
		m = make(map[string]lcs)
		if err := LoadMap("./mimapa.json", &m); err != nil {
			fmt.Println("fallo cargando el fichero mimapa.json")
		}

		for rows.Next() {
			rows.Scan(&scn, &sqlRedo, &operationCode, &xid, &TableName, &Owner)
			sxid := fmt.Sprintf("%x \n", xid)
			switch operationCode {
			case 6: //START or BEGIN TX
				m[sxid] = lcs{Scn: scn, Sql_redo: make([]string, 0), Committed: false, TableName: "", Owner: ""}

			case 1, 2, 3: //viene un INSERT de los redologs
				xp := m[sxid]
				xp.Sql_redo = append(xp.Sql_redo, sqlRedo)
				xp.Owner = Owner
				xp.TableName = TableName
				m[sxid] = xp

			case 7: // viene un COMMIT ...
				xp := m[sxid]
				xp.Committed = true
				xp.Scn = scn
				m[sxid] = xp
			case 36: //viene un rollback
				delete(m, sxid)

			default:
			}
		}

		// dejamos solo los owner que nos interesen
		n = make(map[string]lcs)
		for k, auxLCS := range m {
			if m[k].Owner == "OT" {
				n[k] = auxLCS
			}
		}
		fmt.Println(n)
		fmt.Print("Press 'Enter' to continue...")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		//
		// sorting n, los que tienen ya el commit y los que no
		kscn, kxid := sortedKeys(n)

		// mandamos a Kinesis
		for j, scnkey := range kscn {
			xidtosend := kxid[j]
			// pasamos TX a Kinesis con PutRecord que garantiza el orden
			if n[xidtosend].Committed {
				SendToKinesis(scnkey, xidtosend)
				// borramos TX del map
				delete(n, xidtosend)
				// salvamos map a memoria
				if err := SaveMap("./mimapa.json", n); err != nil {
					fmt.Println("fallo salvando el fichero mimapa.json")
				}
				// guardar en disco los nuevos limites de scn y la ultima xid pasada a kinesis stream
				if err := SaveXid("./mitxt.json", scnkey); err != nil {
					fmt.Println(err)
				}
			}
		}
		// una vez que hemos recorrido todo el map adelantamos el scn al scnmax
		if err := SaveXid("./mitxt.json", scnmax); err != nil {
			fmt.Println(err)
		}

	}

}

func SendToKinesis(sk int64, xid string) {
	fmt.Println(sk, xid)
}

func sortedKeys(mp map[string]lcs) ([]int64, []string) {
	scnkeys := make([]int64, len(mp))
	xidkeys := make([]string, len(mp))
	for idx, mlcs := range mp {
		scnkeys = append(scnkeys, mlcs.Scn)
		xidkeys = append(xidkeys, idx)
	}
	sort.Slice(scnkeys, func(i, j int) bool { return scnkeys[i] < scnkeys[j] })

	return scnkeys, xidkeys
}

func LoadXid(path string, x *int64) error {
	lock.Lock()
	defer lock.Unlock()

	f, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()
	return UnmarshalXid(f, x)

}

var UnmarshalXid = func(r io.Reader, x *int64) error {
	return json.NewDecoder(r).Decode(x)
}

func SaveXid(path string, x int64) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()
	r, err := MarshalXid(x)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	return err
}

var MarshalXid = func(x interface{}) (io.Reader, error) {
	b, err := json.MarshalIndent(x, "", "\t")
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func SaveMap(path string, v map[string]lcs) error {
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()
	r, err := Marshal(v)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	return err
}

func LoadMap(path string, v *map[string]lcs) error {
	lock.Lock()
	defer lock.Unlock()

	f, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()
	return Unmarshal(f, v)

}

// Marshal is a function that marshals the object into an io.Reader.
// By default, it uses the JSON marshaller.
var Marshal = func(v interface{}) (io.Reader, error) {
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

// Unmarshal is a function that unmarshals the data from the
// reader into the specified value.
// By default, it uses the JSON unmarshaller.
var Unmarshal = func(r io.Reader, v *map[string]lcs) error {
	return json.NewDecoder(r).Decode(v)
}
