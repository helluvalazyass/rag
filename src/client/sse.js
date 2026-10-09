const eventSource = new EventSource("http://127.0.0.1:8000/stream")

eventSource.onmessage = event => {
    const parsedData = event.data
    console.log(`Data received: ${parsedData}`)
}

eventSource.onerror = error => {
    console.error("EventSource failed:" , error)
}

eventSource.addEventListener("close", event => {
    console.log("Server is done streaming. Closing client connection!!!")
    eventSource.close();
})