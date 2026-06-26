-- 把 ::: {.jianggao} 围栏 div 包进 LaTeX 的 jianggaoenv 方框环境。
function Div(el)
  for _, c in ipairs(el.classes) do
    if c == 'jianggao' then
      table.insert(el.content, 1, pandoc.RawBlock('latex', '\\begin{jianggaoenv}'))
      table.insert(el.content, pandoc.RawBlock('latex', '\\end{jianggaoenv}'))
      return el.content
    end
  end
end
