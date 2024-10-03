def arr_rotation(arr, k)
  n = arr.length
  return arr if n == 0
  k = k % n
  rotated = Array.new(n)

  for i in 0...n
    new_position = (i + k) % n
    rotated[new_position] = arr[i]
  end

  for i in 0...n
    arr[i] = rotated[i]
  end
end

input = gets.chomp
arr = input.split(',').map(&:to_i)
k = gets.chomp.to_i
arr_rotation(arr, k)
puts arr.inspect