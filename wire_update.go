package mongonet

func (m *UpdateMessage) HasResponse() bool {
	return false
}

func (m *UpdateMessage) Header() MessageHeader {
	return m.header
}

func (m *UpdateMessage) Serialize() []byte {
	size := 16 /* header */ + 8 /* update header */
	size += len(m.Namespace) + 1
	size += int(m.Filter.Size)
	size += int(m.Update.Size)

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	loc := 16

	writeInt32(0, buf, loc)
	loc += 4

	writeCString(m.Namespace, buf, &loc)

	writeInt32(m.Flags, buf, loc)
	loc += 4

	m.Filter.Copy(&loc, buf)
	m.Update.Copy(&loc, buf)

	return buf
}

func parseUpdateMessage(header MessageHeader, buf []byte) (Message, error) {
	m := &UpdateMessage{}
	m.header = header

	var err error
	loc := 0

	m.Reserved = readInt32(buf[loc:])
	loc += 4

	m.Namespace, err = readCString(buf[loc:])
	if err != nil {
		return m, err
	}
	loc += len(m.Namespace) + 1

	m.Flags = readInt32(buf[loc:])
	loc += 4

	m.Filter, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return m, err
	}
	loc += int(m.Filter.Size)

	m.Update, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return m, err
	}
	loc += int(m.Filter.Size)

	return m, nil
}
