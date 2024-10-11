package protocol

type Status string

const (
	SUCCESS Status = "SUCCESS"
	FAILURE Status = "FAILURE"
	PENDING Status = "PENDING"
	STARTED Status = "STARTED"
	RETRY   Status = "RETRY"
)

type Result struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
	DateDone  string        `json:"date_done"` // ISO 8601
}
