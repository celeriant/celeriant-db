use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;

pub async fn echo(mut stream: TcpStream, input: u32) -> std::io::Result<()> {
    let thread_id = std::thread::current().id();
    let fib_result = compute_fibonacci(input);
    let response = format!("[Thread {:?}] fibonacci({}) = {}", thread_id, input, fib_result);
    let response_bytes = response.into_bytes();

    // write all
    stream.write_all(&response_bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_fibonacci_input(stream: &mut TcpStream) -> std::io::Result<Option<u32>> {
    let mut buf = vec![0u8; 8 * 1024];
    let bytes_read = stream.read(&mut buf).await?;
    
    if bytes_read == 0 {
        return Ok(None);
    }
    
    let received_data = String::from_utf8_lossy(&buf[..bytes_read]).trim().to_string();
    Ok(received_data.parse::<u32>().ok())
}

fn compute_fibonacci(n: u32) -> u64 {
    if n <= 1 {
        return n as u64;
    }
    
    // CPU intensive iterative approach with extra work
    let mut a = 0u64;
    let mut b = 1u64;
    
    for i in 2..=n {
        let temp = a.wrapping_add(b);
        a = b;
        b = temp;
        
        // Add some extra CPU work to make it more intensive
        if i % 1000 == 0 {
            // Do some meaningless computation every 1000 iterations
            let _extra_work: u64 = (0..100).map(|x| x * x).sum();
        }
    }
    
    b
}