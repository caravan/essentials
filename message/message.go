package message

type (
	// Message is the base level message passed around by Caravan
	Message interface{}

	// Event is a value that is produced and consumed by Topics
	Event = Message

	// Command is a value used to instruct components to take action
	Command = Message
)
